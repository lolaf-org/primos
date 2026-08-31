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

    /**
     * A follower hearing its leader: a heartbeat arrives, it replies, it probes its peers and
     * restates its intervals. This is what a healthy cluster does all day, and it must cost
     * nothing.
     */
    @Test
    void a_follower_hearing_heartbeats_allocates_nothing() {
        assertThat(bytesPerTurn(node(new ReplayingTransport(heartbeatFrom(PEER_A)))))
                .as("the steady state of every node in a healthy cluster")
                .isLessThan(8L);
    }

    /**
     * The timer path with no campaign in it — a witness has no election timer, so this is the probe
     * round and the advertisement alone.
     */
    @Test
    void probing_and_advertising_allocate_nothing() {
        assertThat(bytesPerTurn(witness(new ReplayingTransport(null))))
                .as("probing every peer and restating the intervals, on a node that never campaigns")
                .isLessThan(8L);
    }

    /**
     * Campaigning is the one path that still allocates, and it is bounded by the election rate
     * rather than by traffic: a set entry per round of pre-votes, and a boxed term in a log call
     * that is built whether or not the level is enabled. A node that cannot reach a leader
     * campaigns here on every turn, which is an election storm and not a steady state, so this is
     * pinned loosely — it exists to catch something new appearing, not to be optimised against.
     */
    @Test
    void campaigning_allocates_only_what_an_election_costs() {
        assertThat(bytesPerTurn(node(new ReplayingTransport(null))))
                .isLessThan(64L);
    }

    /**
     * Bytes allocated per turn, after enough turns to load classes and let the call sites settle.
     */
    private long bytesPerTurn(ElectionNode node) {
        com.sun.management.ThreadMXBean threads = threadMxBean();
        assumeThat(threads).as("this JVM cannot measure per-thread allocation").isNotNull();

        for (int i = 0; i < 200; i++) {
            turn(node);
        }
        long id = Thread.currentThread().getId();
        long before = threads.getThreadAllocatedBytes(id);
        int turns = 2_000;
        for (int i = 0; i < turns; i++) {
            turn(node);
        }
        return (threads.getThreadAllocatedBytes(id) - before) / turns;
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

    private ElectionNode witness(Transport transport) {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(1)
                .asWitness()
                .peers(List.of(Peer.application(PEER_A, "sim", 7001),
                        Peer.application(PEER_B, "sim", 7002)))
                .build();
        ElectionNode node = new ElectionNode(config, new MemoryStateStore(), transport, clock,
                new Random(1L), MessageAuthenticator.disabled());
        node.start();
        return node;
    }

    /**
     * A heartbeat this node will accept, decode and answer — which also resets its election timer,
     * so it behaves as a follower of a live leader rather than campaigning every turn.
     */
    private static ByteBuffer heartbeatFrom(int peerId) {
        MessageEncoder encoder = new MessageEncoder("t", peerId, MessageAuthenticator.disabled());
        ByteBuffer scratch = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
        return copyOf(encoder.appendEntries(scratch, 1));
    }

    private static ByteBuffer copyOf(ByteBuffer encoded) {
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
            if (datagram == null) {
                return 0;
            }
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
