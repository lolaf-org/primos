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
package org.lolaf.primos.transport;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.Peer;
import org.lolaf.primos.SendDropReason;
import org.lolaf.primos.wire.WireFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The real transport: one bound {@link DatagramChannel}, one {@link Selector}, no connections.
 *
 * <p>Raft already assumes a network that drops, delays, duplicates and reorders, and it recovers
 * from all of it with its own timers: a lost vote request is retried by the next election
 * timeout, a lost heartbeat by the next heartbeat interval. TCP's retransmission would duplicate
 * that work while adding head-of-line blocking and connection state that can make failure
 * detection slower. So: UDP, and every message small enough that a datagram is never fragmented.
 *
 * <p>{@link #poll} is where a node spends its life. It waits for a datagram or for the next timer
 * deadline, whichever comes first, which is what lets a whole node run on one thread.
 *
 * <p><strong>Identity is the sender id, never the address.</strong> Source addresses are freely
 * spoofable and, on a multi-homed host, the source of an outgoing datagram is chosen by the
 * routing table rather than by us. Filtering therefore happens above, on the id and the
 * authentication tag; this class only learns where a known peer is currently speaking from.
 *
 * <p><strong>Every peer's hostname is resolved once, when this is built, and a name that does not
 * resolve is a startup failure</strong>: a node that cannot work out where a peer is could never
 * reach it, and broken DNS must not look like a partition. Resolution is the one blocking call
 * here and it happens on the caller's thread, so DNS never touches the node thread.
 *
 * <p><strong>Address learning is the only thing that changes an address at runtime</strong>: a peer
 * that moves is reached again because its own datagrams arrive and say so, which needs
 * {@link Builder#addressLearning} and therefore authentication. A cluster without a secret whose
 * peers change address needs a restart.
 */
@Slf4j
public final class UdpTransport implements Transport {

    /**
     * One byte more than a datagram may legitimately be, so an oversized one is truncated into
     * failing the size check rather than being silently accepted as a shorter message.
     */
    private static final int RECEIVE_CAPACITY = WireFormat.MAX_DATAGRAM_BYTES + 1;

    /** A bound on one turn's reading, or a sustained flood keeps the loop from ever reaching its timers. */
    static final int MAX_DATAGRAMS_PER_POLL = 256;

    private final DatagramChannel channel;
    private final Selector selector;
    private final ByteBuffer receiveBuffer = ByteBuffer.allocate(RECEIVE_CAPACITY);
    private final Map<Integer, InetSocketAddress> addresses = new HashMap<>();
    private final boolean addressLearning;
    private final String clusterName;

    private SocketAddress currentSource;
    private volatile boolean closed;

    /**
     * How many times a peer has been observed at a new address.
     */
    @Getter
    private long addressUpdates;

    private UdpTransport(Builder builder) throws IOException {
        // Checked here rather than in each setter, so that the builder's fields can be plain
        // declarations and every required value is still refused in one place.
        this.addressLearning = builder.addressLearning;
        this.clusterName = Objects.requireNonNull(builder.clusterName, "clusterName");
        Objects.requireNonNull(builder.bindAddress, "bindAddress");
        Objects.requireNonNull(builder.peers, "peers");

        // Before the socket, deliberately: this is the check most likely to fail, and failing it
        // after binding would leak a channel and a selector that nobody is left holding.
        resolveAll(builder.peers);

        this.channel = DatagramChannel.open();
        try {
            this.channel.configureBlocking(false);
            if (builder.receiveBufferBytes > 0) {
                setReceiveBuffer(builder.receiveBufferBytes);
            }
            this.channel.bind(builder.bindAddress);
            this.selector = Selector.open();
            this.channel.register(selector, SelectionKey.OP_READ);
        } catch (IOException | RuntimeException e) {
            this.channel.close();
            throw e;
        }

        log.info("cluster '{}': bound to {}", clusterName, channel.getLocalAddress());
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Resolves every peer, or refuses to build.
     *
     * <p>All of them are tried before anything is thrown, and the failures are reported together.
     * Stopping at the first would make an operator who mistyped three hostnames find them one
     * restart at a time.
     *
     * @throws IllegalStateException if any peer's hostname does not resolve
     */
    private void resolveAll(List<Peer> peers) {
        List<String> unresolved = new ArrayList<>();
        for (Peer peer : peers) {
            try {
                InetAddress host = InetAddress.getByName(peer.host());
                addresses.put(peer.id(), new InetSocketAddress(host, peer.port()));
            } catch (UnknownHostException | SecurityException e) {
                unresolved.add("peer " + peer.id() + " (" + peer.name() + ") at "
                        + peer.host() + ":" + peer.port());
            }
        }
        if (!unresolved.isEmpty()) {
            throw new IllegalStateException("cluster '" + clusterName + "': cannot resolve the"
                    + " address of " + unresolved.size() + " of " + peers.size() + " peers, so"
                    + " this node could never reach them: " + String.join(", ", unresolved)
                    + ". A name that does not resolve is a configuration error, not a network"
                    + " condition: fix it or use an IP address.");
        }
    }

    /**
     * The address actually bound, useful when the port was left to the operating system.
     */
    public InetSocketAddress boundAddress() {
        try {
            return (InetSocketAddress) channel.getLocalAddress();
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the bound address", e);
        }
    }

    @Override
    public SendDropReason send(int peerId, ByteBuffer datagram) {
        // A failed send is logged at debug: heartbeats are every 50ms per peer, so a visible level
        // would turn a broken peer into a second outage.
        if (closed) {
            return SendDropReason.TRANSPORT_CLOSED;
        }
        InetSocketAddress target = addresses.get(peerId);
        if (target == null) {
            // Not a network condition: this node was asked for a peer it was never configured
            // with, which is a bug in the caller rather than a lost datagram. Every configured
            // peer has an address, because one that did not resolve refused to build.
            log.warn("cluster '{}': asked to send to peer {}, which is not a configured peer", clusterName, peerId);
            return SendDropReason.NO_ADDRESS;
        }
        try {
            // Always from the bound socket, never a transient one, so a peer sees this node's
            // listening endpoint as the source. That is what makes address learning correct and
            // keeps a NAT pinhole usable in both directions.
            int bytes = datagram.remaining();
            if (channel.send(datagram, target) < bytes) {
                if (log.isDebugEnabled()) {
                    log.debug("cluster '{}': no room in the socket to send to peer {} at {}", clusterName, peerId, target);
                }
                return SendDropReason.SOCKET_ERROR;
            }
            return null;
        } catch (IOException | RuntimeException e) {
            // Sends fail routinely on UDP, and an ICMP port-unreachable can surface here long
            // after the fact. The algorithm's timers are the retry mechanism, so this must never
            // become a node failure.
            if (log.isDebugEnabled()) {
                log.debug("cluster '{}': send to peer {} at {} failed: {}", clusterName, peerId, target, e.toString());
            }
            return SendDropReason.SOCKET_ERROR;
        }
    }

    @Override
    public int poll(long timeoutNanos, DatagramHandler handler) {
        if (closed) {
            return 0;
        }
        try {
            if (timeoutNanos <= 0) {
                selector.selectNow();
            } else {
                // select(0) blocks forever in NIO, so a sub-millisecond wait is rounded up rather
                // than turned into an indefinite one.
                selector.select(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(timeoutNanos)));
            }
            selector.selectedKeys().clear();
            return drain(handler);
        } catch (IOException e) {
            log.warn("cluster '{}': NIO select failed", clusterName, e);
            return 0;
        }
    }

    private int drain(DatagramHandler handler) throws IOException {
        int delivered = 0;
        while (!closed && delivered < MAX_DATAGRAMS_PER_POLL) {
            receiveBuffer.clear();
            SocketAddress source = channel.receive(receiveBuffer);
            if (source == null) {
                return delivered;
            }
            receiveBuffer.flip();
            currentSource = source;
            handler.onDatagram(receiveBuffer);
            currentSource = null;
            delivered++;
        }
        return delivered;
    }

    /**
     * Records where a peer is speaking from, now that the message has passed every check.
     *
     * <p>Learning is why a peer that restarts on a new address can be reached again without an
     * operator editing configuration, and since hostnames are resolved once at construction it is
     * the only thing that updates an address at all. It is gated on authentication for a reason:
     * learning from an unauthenticated datagram would let a single spoofed packet redirect a
     * peer's traffic and partition it, without the attacker forging any content at all. The tag
     * does not cover the source address, so a captured datagram replayed from elsewhere still
     * redirects that peer until it next speaks.
     */
    @Override
    public void confirmSender(int senderId) {
        if (!addressLearning || currentSource == null) {
            return;
        }
        InetSocketAddress known = addresses.get(senderId);
        if (known == null || !(currentSource instanceof InetSocketAddress)) {
            return;
        }
        InetSocketAddress observed = (InetSocketAddress) currentSource;
        if (!observed.equals(known)) {
            addresses.put(senderId, observed);
            addressUpdates++;
            log.info("cluster '{}': peer {} is now speaking from {}",
                    clusterName, senderId, observed);
        }
    }

    @Override
    public void wakeup() {
        selector.wakeup();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            selector.wakeup();
            channel.close();
        } catch (IOException e) {
            log.debug("closing the channel failed", e);
        }
        try {
            selector.close();
        } catch (IOException e) {
            log.debug("closing the selector failed", e);
        }
    }

    private void setReceiveBuffer(int bytes) {
        try {
            channel.setOption(StandardSocketOptions.SO_RCVBUF, bytes);
        } catch (IOException | UnsupportedOperationException | IllegalArgumentException e) {
            log.debug("this platform does not support SO_RCVBUF: {}", e.toString());
        }
    }

    /**
     * Builds a {@link UdpTransport}.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Builder implements Supplier<Transport> {

        /**
         * Where to listen. Defaults to the wildcard address on an operating-system-chosen port.
         */
        @Setter
        private InetSocketAddress bindAddress = new InetSocketAddress(0);

        /**
         * The peers this node may send to, the witness included.
         */
        @Setter
        private List<Peer> peers = Collections.emptyList();

        /**
         * Whether a peer's address may be updated from received traffic.
         *
         * <p>Enable this only when authentication is on. Without a tag, one spoofed datagram
         * redirects a peer's traffic and partitions it without forging any content.
         */
        @Setter
        private boolean addressLearning;

        /**
         * Socket receive buffer. Sized generously on purpose: a silently overflowing socket looks
         * exactly like a partitioned network.
         */
        @Setter
        private int receiveBufferBytes = 1 << 20;

        /**
         * Used only in log lines.
         */
        @Setter
        private String clusterName = "primos";

        /**
         * Binds the socket and resolves every peer.
         *
         * @throws IllegalStateException if a peer's hostname does not resolve. Deliberately fatal
         *                               and deliberately here, on the calling thread, before the
         *                               node has one of its own; see the class javadoc.
         * @throws IOException           if the socket cannot be opened or bound
         */
        public UdpTransport build() throws IOException {
            return new UdpTransport(this);
        }

        /**
         * Builds a transport, wrapping the {@code IOException} so this can be handed to
         * {@code PrimosNode.builder().transport(...)} directly.
         *
         * <p>A node takes a <em>supplier</em> of transports rather than a transport, because a
         * stopped node holds a closed socket and starting it again needs a new one. That would
         * otherwise force every caller to write the same try-and-wrap lambda around this builder,
         * so the builder is the supplier and writes it once.
         *
         * <p>Each call binds a fresh socket, which is exactly what a restart wants. A builder is
         * reusable for that reason: nothing here is consumed by building.
         */
        @Override
        public Transport get() {
            try {
                return build();
            } catch (IOException e) {
                throw new UncheckedIOException("cannot bind the primos socket for cluster '"
                        + clusterName + "'", e);
            }
        }
    }
}
