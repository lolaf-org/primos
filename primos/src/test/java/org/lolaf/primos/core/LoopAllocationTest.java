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
package org.lolaf.primos.core;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.DatagramHandler;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.WireFormat;

/**
 * The loop path must not allocate to say that nothing changed.
 *
 * <p>The rule is stated all over this project and was, until this test, enforced by nobody: a stats
 * snapshot nobody asked for, a {@code RoleAndTerm} identical to the last one, an iterator per
 * traversal of the peers, an {@link Integer} boxed to check a sender id. Every one of those is
 * invisible in behaviour and shows up only as a node thread quietly producing garbage at heartbeat
 * rate.
 *
 * <p>The transport is a stub that discards sends and replays one prepared datagram, so what is
 * measured is the node and not the harness — a simulated network copies buffers into queues, and
 * that noise would drown the thing under test.
 *
 * <p>The bound is a ratchet rather than a target: it is set just under what this cost before the
 * peers became an array, so a reversion fails and an improvement invites tightening. The remaining
 * allocation is named at the assertion.
 *
 * <p><strong>Peer ids are deliberately above 127.</strong> {@code Integer.valueOf} caches
 * −128..127, so a boxing regression is invisible with the ids tests usually use and appears only
 * for the sparse numbering the documentation recommends.
 */
class LoopAllocationTest {

    /** Hoisted, so the harness allocates nothing inside the measured loop. */
    private static final Duration STEP = Duration.ofSeconds(31);

    private static final int PEER_A = 201;
    private static final int PEER_B = 202;

    private final VirtualClock clock = new VirtualClock();

    @Test
    void a_turn_of_the_loop_allocates_almost_nothing() {
        com.sun.management.ThreadMXBean threads = threadMxBean();
        assumeThat(threads).as("this JVM cannot measure per-thread allocation").isNotNull();

        ReplayingTransport transport = new ReplayingTransport(advertisementFrom(PEER_A));
        ElectionNode node = node(transport);

        // Warm up: first turns load classes, resolve call sites and log the startup lines.
        for (int i = 0; i < 200; i++) {
            turn(node);
        }

        long id = Thread.currentThread().getId();
        long before = threads.getThreadAllocatedBytes(id);
        int turns = 2_000;
        for (int i = 0; i < turns; i++) {
            turn(node);
        }
        long perTurn = (threads.getThreadAllocatedBytes(id) - before) / turns;

        // Measured at each step, not guessed:
        //   203  a List<Peer> of peers beside a LinkedHashMap<Integer, Probe> of their state
        //   121  peers as a Peer[], the Set<Integer> sender check gone
        //    76  the two structures merged into one PeerState[], no map left to box or iterate
        //    60  the receive callback hoisted out of the call site it was rebuilt at every turn
        // The bound is a ratchet: comfortably above what it costs now, comfortably below any of
        // the shapes above it, so a reversion fails and an improvement invites tightening.
        assertThat(perTurn)
                .as("a turn receives a datagram, probes two peers and re-advertises; an iterator"
                        + " per traversal, a boxed sender id or a lambda per call shows up here")
                .isLessThan(96L);
    }

    /** One turn: enough virtual time that the probe and advertisement deadlines both fire. */
    private void turn(ElectionNode node) {
        clock.advance(STEP);
        node.runOnce(0);
    }

    private ElectionNode node(Transport transport) {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(1)
                .peers(List.of(Peer.application(PEER_A, "sim", 7001),
                        Peer.application(PEER_B, "sim", 7002)))
                .build();
        ElectionNode node = new ElectionNode(config, new MemoryStateStore(), transport, clock,
                new Random(1L), MessageAuthenticator.disabled());
        node.start();
        return node;
    }

    /** A datagram this node will accept, decode and dispatch, exercising the sender check. */
    private static ByteBuffer advertisementFrom(int peerId) {
        MessageEncoder encoder = new MessageEncoder("t", peerId, MessageAuthenticator.disabled());
        ByteBuffer scratch = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
        ByteBuffer encoded = encoder.timings(scratch, 0, 50, 300, 600, 1000, 3);
        ByteBuffer copy = ByteBuffer.allocate(encoded.remaining());
        copy.put(encoded).flip();
        return copy;
    }

    private static com.sun.management.ThreadMXBean threadMxBean() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean)) {
            return null;
        }
        com.sun.management.ThreadMXBean sun = (com.sun.management.ThreadMXBean) bean;
        if (!sun.isThreadAllocatedMemorySupported()) {
            return null;
        }
        sun.setThreadAllocatedMemoryEnabled(true);
        return sun;
    }

    /**
     * Discards sends and hands back the same datagram on every poll. Allocation-free itself, so
     * whatever the measurement sees belongs to the node.
     */
    private static final class ReplayingTransport implements Transport {

        private final ByteBuffer datagram;

        private ReplayingTransport(ByteBuffer datagram) {
            this.datagram = datagram;
        }

        @Override
        public void send(int peerId, ByteBuffer datagram) {
        }

        @Override
        public int poll(long timeoutNanos, DatagramHandler handler) {
            datagram.position(0).limit(datagram.capacity());
            handler.onDatagram(datagram);
            return 1;
        }

        @Override
        public long sendsDropped() {
            return 0;
        }

        @Override
        public void wakeup() {
        }

        @Override
        public void close() {
        }
    }
}
