/*
 * Copyright © 2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.primos.witness;

import lombok.AccessLevel;
import lombok.Getter;

import org.lolaf.primos.Peer;
import org.lolaf.primos.transport.DatagramHandler;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Selector;
import java.util.*;
import java.util.Collections;
import java.util.Set;

/**
 * One application's view of the witness server's single socket.
 *
 * <p>The server owns one {@link DatagramChannel} for every cluster it serves, because binding a
 * port per tenant would make onboarding a firewall change. So the socket cannot be handed to an
 * {@code ElectionNode} directly: a node expects a transport that delivers only its own traffic.
 * This is that transport — an inbox the server pushes into after reading the application name, and
 * a {@code send} that writes back to the shared channel.
 *
 * <p><strong>Addresses are learned, never configured.</strong> The witness holds no addresses at
 * all — only application names and secrets — so the only way it can reply to anybody is to
 * remember where they spoke from. Learning happens in {@link #confirmSender}, which the node calls
 * only after a datagram has passed authentication, because learning from an unverified one would
 * let a single spoofed packet redirect a node's traffic and partition it without forging any
 * content.
 *
 * <p>The inbox is bounded and copies. Copying costs an allocation-free memcpy into a pooled buffer
 * and buys the thing that matters on shared infrastructure: a datagram delivered inside
 * {@code runOnce}, where a node's own catch-all contains a failure to that node. Delivering
 * straight from the server's read loop would let one tenant's broken disk throw through the server
 * and take down every other tenant with it. The bound is the other half of the same argument —
 * one noisy cluster must not be able to grow the server's heap.
 *
 * <p>Owned by the server thread, like every other transport here.
 */
final class RoutedTransport implements Transport {

    /**
     * How many datagrams may wait for one application. Reached only if a node's traffic outruns
     * the server loop, which for a witness answering a few messages a second means something has
     * gone wrong rather than that the buffer is too small.
     */
    static final int INBOX_CAPACITY = 256;
    private static final Logger log = LoggerFactory.getLogger(RoutedTransport.class);
    private final String application;
    private final DatagramChannel channel;
    private final Selector selector;

    private final Deque<Inbound> queued = new ArrayDeque<>();
    private final Deque<Inbound> free = new ArrayDeque<>();
    private final Map<Integer, InetSocketAddress> endpoints = new LinkedHashMap<>();
    private final List<Peer> learned = new ArrayList<>();

    private InetSocketAddress currentSource;
    private boolean closed;

    private long overflowed;

    /**
     * Replies this application could not address to anybody.
     *
     * <p>Expected to stay at zero. A witness only ever answers, and an answer is addressed to the
     * source of the datagram being answered even before that node has been learned — so anything
     * counted here means a send went looking for a node outside a reply, which a witness has no
     * reason to do.
     */
    @Getter(AccessLevel.PACKAGE)
    private long undeliverable;

    private long sendFailures;
    private long sendsWhileClosed;

    RoutedTransport(String application, DatagramChannel channel, Selector selector) {
        this.application = application;
        this.channel = channel;
        this.selector = selector;
    }

    /**
     * Queues a datagram the server has routed here, copying it out of the shared receive buffer.
     *
     * @return whether it was queued rather than dropped for a full inbox
     */
    boolean offer(ByteBuffer datagram, InetSocketAddress source) {
        if (closed) {
            return false;
        }
        if (queued.size() >= INBOX_CAPACITY) {
            overflowed++;
            if (overflowed == 1 || overflowed % 1000 == 0) {
                log.warn("cluster '{}': the witness inbox is full ({} datagrams) and is dropping"
                                + " traffic; {} dropped so far",
                        application, INBOX_CAPACITY, overflowed);
            }
            return false;
        }
        Inbound slot = free.pollFirst();
        if (slot == null) {
            slot = new Inbound();
        }
        slot.set(datagram, source);
        queued.addLast(slot);
        return true;
    }

    @Override
    public void send(int peerId, ByteBuffer datagram) {
        if (closed) {
            sendsWhileClosed++;
            return;
        }
        InetSocketAddress target = endpoints.get(peerId);
        if (target == null) {
            // The first datagram a node ever sends is answered here, and it has to be. The node
            // only becomes known through confirmSender, which the decoder calls *after* dispatch —
            // so while the reply to a node's first request is being written there is still no
            // learned address for it, and without this the very first request from every node
            // would go unanswered and cost an election timeout to retry.
            //
            // Replying to the source of the datagram currently being handled is safe for the same
            // reason confirmSender is: a handler only runs once the tag has been verified, so
            // currentSource here is an authenticated peer's address rather than a claimed one.
            target = currentSource;
        }
        if (target == null) {
            // Not a reply, and nobody to send to. The witness only ever answers, so it can have no
            // address for a node that has never spoken to it.
            undeliverable++;
            return;
        }
        try {
            channel.send(datagram, target);
        } catch (IOException | RuntimeException e) {
            // Routine on UDP, and an ICMP port-unreachable can surface here long after the fact.
            // The algorithm's timers are the retry mechanism, so this must never become a failure
            // — but it is counted, because a reply this witness never managed to send is the
            // difference between a cluster failing over and a cluster waiting.
            sendFailures++;
            if (log.isDebugEnabled()) {
                log.debug("cluster '{}': send to node {} at {} failed: {}",
                        application, peerId, target, e.toString());
            }
        }
    }

    @Override
    public int poll(long timeoutNanos, DatagramHandler handler) {
        // The timeout is ignored on purpose: the server does the one blocking select for every
        // application at once, because a node per tenant each waiting on its own deadline would be
        // a thread per tenant. By the time this is called the inbox already holds whatever arrived.
        int delivered = 0;
        while (!closed) {
            Inbound inbound = queued.pollFirst();
            if (inbound == null) {
                break;
            }
            currentSource = inbound.source;
            try {
                handler.onDatagram(inbound.buffer());
            } finally {
                currentSource = null;
                inbound.clear();
                free.addLast(inbound);
            }
            delivered++;
        }
        return delivered;
    }

    @Override
    public void wakeup() {
        selector.wakeup();
    }

    /**
     * Remembers where a node is speaking from, now that its datagram has passed every check.
     *
     * <p>This is the entire addressing story for a witness. It is also why a node that restarts on
     * a new address can be answered again with no operator involvement, and why the tag has to be
     * verified first: without that, one spoofed datagram would redirect a node's replies.
     */
    @Override
    public void confirmSender(int senderId) {
        if (closed || currentSource == null) {
            return;
        }
        InetSocketAddress observed = currentSource;
        InetSocketAddress previous = endpoints.put(senderId, observed);
        if (observed.equals(previous)) {
            return;
        }
        if (previous != null) {
            log.info("cluster '{}': node {} is now speaking from {} rather than {}",
                    application, senderId, observed, previous);
        }
        learned.add(Peer.application(senderId, observed.getHostString(), observed.getPort()));
    }

    /**
     * Takes the voters learned since the last call, for the server to hand to the node.
     *
     * <p>Drained between turns of the loop rather than applied inside one, so a node's peer set is
     * never mutated while the node is in the middle of using it.
     */
    List<Peer> drainLearned() {
        if (learned.isEmpty()) {
            return Collections.emptyList();
        }
        List<Peer> drained = new ArrayList<>(learned);
        learned.clear();
        return drained;
    }

    /**
     * Every node this application has been heard from, and where.
     *
     * <p>Read when a node is rebuilt after a secret rotation, so the replacement inherits the
     * voters this one had learned rather than waiting for each to speak again.
     */
    Set<Map.Entry<Integer, InetSocketAddress>> endpoints() {
        return new LinkedHashMap<>(endpoints).entrySet();
    }

    /**
     * Datagrams dropped because this application's inbox was full.
     */
    long overflowed() {
        return overflowed;
    }

    /**
     * Every reply this transport did not put on the wire, for any reason.
     *
     * <p>Counted for the same reason the node's transport counts them: a send that returns without
     * either sending or recording anything is indistinguishable from one that worked, and a
     * witness that silently fails to answer looks exactly like a witness nobody is asking.
     */
    long sendsDropped() {
        return undeliverable + sendFailures + sendsWhileClosed;
    }

    /**
     * Drops the queue. The channel and selector belong to the server and are left alone.
     */
    @Override
    public void close() {
        closed = true;
        queued.clear();
        free.clear();
        learned.clear();
    }

    /**
     * One queued datagram and the address it came from, reused rather than reallocated.
     */
    private static final class Inbound {

        private final ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES + 1);
        private InetSocketAddress source;

        private void set(ByteBuffer datagram, InetSocketAddress from) {
            buffer.clear();
            buffer.put(datagram.duplicate());
            buffer.flip();
            this.source = from;
        }

        private ByteBuffer buffer() {
            buffer.position(0);
            return buffer;
        }

        private void clear() {
            source = null;
        }
    }
}
