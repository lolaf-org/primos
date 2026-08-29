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

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.Peer;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.wire.WireFormat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The real transport: one bound {@link DatagramChannel}, one {@link Selector}, no connections.
 *
 * <p>Raft already assumes a network that drops, delays, duplicates and reorders, and it recovers
 * from all of it with its own timers — a lost vote request is retried by the next election
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
 */
@Slf4j
public final class UdpTransport implements Transport {

    /**
     * One byte more than a datagram may legitimately be, so an oversized one is truncated into
     * failing the size check rather than being silently accepted as a shorter message.
     */
    private static final int RECEIVE_CAPACITY = WireFormat.MAX_DATAGRAM_BYTES + 1;

    private final DatagramChannel channel;
    private final Selector selector;
    private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(RECEIVE_CAPACITY);
    private final Map<Integer, Endpoint> endpoints = new HashMap<>();
    private final boolean addressLearning;
    private final Duration reresolveInterval;
    private final String clusterName;
    private final MonotonicClock clock;

    private SocketAddress currentSource;
    private volatile boolean closed;

    private long sendFailures;
    private long sendsWhileClosed;
    private long sendsToUnknownPeer;
    private long sendsWithoutAddress;
    private long addressUpdates;

    private UdpTransport(Builder builder) throws IOException {
        // Checked here rather than in each setter, so that the builder's fields can be plain
        // declarations and every required value is still refused in one place.
        this.addressLearning = builder.addressLearning;
        this.reresolveInterval = Objects.requireNonNull(builder.reresolveInterval, "reresolveInterval");
        this.clusterName = Objects.requireNonNull(builder.clusterName, "clusterName");
        this.clock = Objects.requireNonNull(builder.clock, "clock");
        Objects.requireNonNull(builder.bindAddress, "bindAddress");
        Objects.requireNonNull(builder.peers, "peers");

        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
        setIfSupported(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
        if (builder.receiveBufferBytes > 0) {
            setIfSupported(StandardSocketOptions.SO_RCVBUF, builder.receiveBufferBytes);
        }
        this.channel.bind(builder.bindAddress);
        this.selector = Selector.open();
        this.channel.register(selector, SelectionKey.OP_READ);

        for (Peer peer : builder.peers) {
            endpoints.put(peer.id(), new Endpoint(peer));
        }
        log.info("cluster '{}': bound to {}", clusterName, channel.getLocalAddress());
    }

    public static Builder builder() {
        return new Builder();
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
    public void send(int peerId, ByteBuffer datagram) {
        // Every path out of here that does not put bytes on the wire is counted, because the SPI
        // promises a failed send is counted and logged rather than propagated — and a send that
        // returns without doing either is indistinguishable from a working one. Logging is at
        // debug throughout: heartbeats are every 50ms per peer, so a visible level would turn a
        // broken peer into a second outage.
        if (closed) {
            sendsWhileClosed++;
            return;
        }
        Endpoint endpoint = endpoints.get(peerId);
        if (endpoint == null) {
            // Not a network condition: this node was asked for a peer it was never configured
            // with, which is a bug in the caller rather than a lost datagram.
            sendsToUnknownPeer++;
            if (log.isDebugEnabled()) {
                log.debug("cluster '{}': asked to send to {}, which is not a configured peer",
                        clusterName, peerId);
            }
            return;
        }
        InetSocketAddress target = endpoint.address();
        if (target == null) {
            // The hostname has never resolved. Without this counter the peer simply looks
            // unreachable, with nothing anywhere to say that DNS is the reason.
            sendsWithoutAddress++;
            endpoint.resolveInBackground();
            if (log.isDebugEnabled()) {
                log.debug("cluster '{}': no address yet for peer {} ({}); dropping this datagram",
                        clusterName, peerId, endpoint.peer.host());
            }
            return;
        }
        try {
            // Always from the bound socket, never a transient one, so a peer sees this node's
            // listening endpoint as the source. That is what makes address learning correct and
            // keeps a NAT pinhole usable in both directions.
            channel.send(datagram, target);
        } catch (IOException | RuntimeException e) {
            // Sends fail routinely on UDP, and an ICMP port-unreachable can surface here long
            // after the fact. The algorithm's timers are the retry mechanism, so this must never
            // become a node failure.
            sendFailures++;
            endpoint.resolveInBackground();
            if (log.isDebugEnabled()) {
                log.debug("cluster '{}': send to {} at {} failed: {}",
                        clusterName, peerId, target, e.toString());
            }
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
                long millis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(timeoutNanos));
                selector.select(millis);
            }
            selector.selectedKeys().clear();
            return drain(handler);
        } catch (IOException e) {
            log.debug("cluster '{}': select failed: {}", clusterName, e.toString());
            return 0;
        }
    }

    private int drain(DatagramHandler handler) throws IOException {
        int delivered = 0;
        while (!closed) {
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
     * operator editing configuration. It is gated on authentication for a reason: learning from an
     * unauthenticated datagram would let a single spoofed packet redirect a peer's traffic and
     * partition it, without the attacker forging any content at all.
     */
    @Override
    public void confirmSender(int senderId) {
        if (!addressLearning || currentSource == null) {
            return;
        }
        Endpoint endpoint = endpoints.get(senderId);
        if (endpoint == null || !(currentSource instanceof InetSocketAddress)) {
            return;
        }
        InetSocketAddress observed = (InetSocketAddress) currentSource;
        if (endpoint.learn(observed)) {
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

    /**
     * How many sends the socket itself rejected. Routine on UDP; a rising rate is not.
     */
    public long sendFailures() {
        return sendFailures;
    }

    /**
     * Every datagram this transport did not put on the wire, for any reason.
     *
     * <p>The one number worth alerting on. A healthy node's is flat: the individual counters below
     * separate a network that is refusing traffic from a name that will not resolve from a bug in
     * the caller, and those want very different responses.
     */
    public long sendsDropped() {
        return sendFailures + sendsWhileClosed + sendsToUnknownPeer + sendsWithoutAddress;
    }

    /**
     * Sends attempted after {@link #close}. Expected during shutdown, and harmless.
     */
    public long sendsWhileClosed() {
        return sendsWhileClosed;
    }

    /**
     * Sends addressed to a peer this transport was not configured with.
     *
     * <p>Should stay at zero. Unlike the others this is not a network condition — it means
     * something asked for a peer id that was never in the configuration.
     */
    public long sendsToUnknownPeer() {
        return sendsToUnknownPeer;
    }

    /**
     * Sends dropped because the peer's hostname has never resolved.
     *
     * <p>The quietest failure of the set, and the reason these counters exist: without it a peer
     * whose DNS is broken is simply unreachable, with nothing to distinguish that from a network
     * partition or a dead process.
     */
    public long sendsWithoutAddress() {
        return sendsWithoutAddress;
    }

    /**
     * How many times a peer has been observed at a new address.
     */
    public long addressUpdates() {
        return addressUpdates;
    }

    private <T> void setIfSupported(SocketOption<T> option, T value) {
        try {
            channel.setOption(option, value);
        } catch (IOException | UnsupportedOperationException | IllegalArgumentException e) {
            log.debug("this platform does not support {}: {}", option.name(), e.toString());
        }
    }

    /**
     * Builds a {@link UdpTransport}.
     */
    public static final class Builder {

        /**
         * Where to listen. Defaults to the wildcard address on an operating-system-chosen port.
         */
        @Setter
        private InetSocketAddress bindAddress = new InetSocketAddress(0);

        /**
         * The peers this node may send to, the witness included.
         */
        @Setter
        private List<Peer> peers = java.util.Collections.emptyList();

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
         * How often a peer's hostname is looked up again.
         */
        @Setter
        private Duration reresolveInterval = Duration.ofSeconds(60);

        /**
         * Used only in log lines.
         */
        @Setter
        private String clusterName = "primos";

        /**
         * The clock used to decide when a hostname is worth looking up again.
         *
         * <p>Here for the same reason it is everywhere else: nothing in primos reads the system
         * clock directly, so that the rule has no exceptions to remember.
         */
        @Setter
        private MonotonicClock clock = MonotonicClock.SYSTEM;

        private Builder() {
        }

        public UdpTransport build() throws IOException {
            return new UdpTransport(this);
        }
    }

    /**
     * Where one peer is, and when that was last worked out.
     */
    private final class Endpoint {

        private final Peer peer;
        private final AtomicBoolean resolving = new AtomicBoolean();

        private volatile InetSocketAddress address;
        private volatile long resolvedAtNanos = Long.MIN_VALUE;

        private Endpoint(Peer peer) {
            this.peer = peer;
            // Resolved here, on the caller's thread, so the very first datagram has somewhere to
            // go. Only refreshes happen in the background, where blocking would cost heartbeats.
            InetSocketAddress initial = new InetSocketAddress(peer.host(), peer.port());
            if (initial.isUnresolved()) {
                log.warn("cluster '{}': cannot resolve {} for peer {} yet; will retry in the"
                        + " background", clusterName, peer.host(), peer.id());
                this.address = null;
            } else {
                this.address = initial;
            }
            this.resolvedAtNanos = clock.nanoTime();
        }

        private InetSocketAddress address() {
            InetSocketAddress current = address;
            if (current != null && clock.nanoTime() - resolvedAtNanos > reresolveInterval.toNanos()) {
                resolveInBackground();
            }
            return current;
        }

        private boolean learn(InetSocketAddress observed) {
            InetSocketAddress current = address;
            if (observed.equals(current)) {
                return false;
            }
            address = observed;
            resolvedAtNanos = clock.nanoTime();
            return true;
        }

        /**
         * Resolves the hostname on a short-lived worker.
         *
         * <p>Never on the node thread: a DNS lookup can block for seconds, and the node has
         * heartbeats to send. Re-resolution is rare, so the steady state remains one thread.
         */
        private void resolveInBackground() {
            if (closed || !resolving.compareAndSet(false, true)) {
                return;
            }
            Thread worker = new Thread(() -> {
                try {
                    InetSocketAddress resolved =
                            new InetSocketAddress(peer.host(), peer.port());
                    if (resolved.isUnresolved()) {
                        log.debug("cluster '{}': cannot resolve {} for peer {}",
                                clusterName, peer.host(), peer.id());
                    } else {
                        // Closes the loop on the warning logged at construction: an operator who
                        // was told a name would not resolve should also be told when it does.
                        if (address == null) {
                            log.info("cluster '{}': {} now resolves for peer {}; sending resumes",
                                    clusterName, peer.host(), peer.id());
                        }
                        address = resolved;
                    }
                    resolvedAtNanos = clock.nanoTime();
                } catch (RuntimeException e) {
                    log.debug("cluster '{}': resolving {} failed: {}",
                            clusterName, peer.host(), e.toString());
                } finally {
                    resolving.set(false);
                }
            }, "primos-resolve-" + peer.id());
            worker.setDaemon(true);
            worker.start();
        }
    }
}
