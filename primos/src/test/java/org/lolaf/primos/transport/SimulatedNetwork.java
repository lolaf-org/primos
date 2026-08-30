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

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import org.lolaf.primos.time.VirtualClock;

/**
 * A network that misbehaves on purpose, reproducibly.
 *
 * <p>This is where primos is actually tested. Wall-clock multi-threaded tests find the bugs that
 * happen to surface; a seeded network that drops, delays, reorders and partitions on demand finds
 * the ones that matter, and finds them the same way twice. Every bug this uncovers should have its
 * seed checked in as a regression.
 *
 * <p>Everything is deterministic given the seed: delays come from one {@link Random}, and messages
 * queued for the same instant are delivered in send order, so a run can be replayed exactly.
 *
 * <p>Nothing here advances time. The harness owns the clock and calls {@link Transport#poll},
 * which delivers whatever has come due — so a partition lasting four election timeouts costs no
 * wall-clock time at all.
 */
public final class SimulatedNetwork {

    private final VirtualClock clock;
    private final Random random;
    private final Map<Integer, PriorityQueue<InFlight>> mailboxes = new HashMap<>();
    private final Set<Long> severed = new HashSet<>();

    private double lossProbability;
    private long minDelayNanos;
    private long maxDelayNanos;
    private long sequence;

    @Getter
    private long sent;

    @Getter
    private long delivered;

    @Getter
    private long droppedByLoss;
    @Getter
    private long droppedByPartition;

    public SimulatedNetwork(long seed, VirtualClock clock) {
        this.clock = clock;
        this.random = new Random(seed);
    }

    /** A transport for one node. Nodes need not be declared in advance. */
    public Transport transportFor(int nodeId) {
        mailboxes.computeIfAbsent(nodeId, id -> new PriorityQueue<>());
        return new SimulatedTransport(nodeId);
    }

    /** Fraction of datagrams to discard, from 0.0 to 1.0. */
    public void lossProbability(double probability) {
        if (probability < 0 || probability > 1) {
            throw new IllegalArgumentException("probability must be within 0..1, was " + probability);
        }
        this.lossProbability = probability;
    }

    /**
     * The range each datagram's delivery is delayed by. A range wider than a heartbeat interval is
     * what makes messages reorder, which is the interesting case.
     */
    public void delayRange(long minNanos, long maxNanos) {
        if (minNanos < 0 || maxNanos < minNanos) {
            throw new IllegalArgumentException("delay range must be 0 <= min <= max");
        }
        this.minDelayNanos = minNanos;
        this.maxDelayNanos = maxNanos;
    }

    /**
     * Splits the network, discarding everything crossing between the two groups in either
     * direction. Members within a group still reach each other.
     */
    public void partition(Collection<Integer> sideA, Collection<Integer> sideB) {
        for (int a : sideA) {
            for (int b : sideB) {
                sever(a, b);
                sever(b, a);
            }
        }
    }

    /** Cuts every link into and out of a node, the common "this box fell off the network" case. */
    public void isolate(int nodeId) {
        for (int other : new ArrayList<>(mailboxes.keySet())) {
            if (other != nodeId) {
                sever(nodeId, other);
                sever(other, nodeId);
            }
        }
    }

    /**
     * Cuts one direction only.
     *
     * <p>Worth reaching for: a one-way link is where consensus implementations tend to break,
     * because a node can be heard while hearing nothing, or the reverse.
     */
    public void sever(int from, int to) {
        severed.add(link(from, to));
    }

    /** Restores one direction. */
    public void restore(int from, int to) {
        severed.remove(link(from, to));
    }

    /** Restores every link. */
    public void heal() {
        severed.clear();
    }

    /** Whether a datagram sent now would be discarded by a partition. */
    public boolean isSevered(int from, int to) {
        return severed.contains(link(from, to));
    }

    /**
     * When the next queued datagram comes due, or {@link Long#MAX_VALUE} if none is queued. The
     * harness uses this to jump the clock straight to the next thing that happens.
     */
    public long nextDeliveryNanos() {
        long earliest = Long.MAX_VALUE;
        for (PriorityQueue<InFlight> mailbox : mailboxes.values()) {
            InFlight head = mailbox.peek();
            if (head != null && head.dueNanos < earliest) {
                earliest = head.dueNanos;
            }
        }
        return earliest;
    }

    /**
     * When the next datagram queued <em>for one node</em> comes due, or {@link Long#MAX_VALUE} if
     * none is. The scheduler asks per node rather than globally, because a node that is down will
     * never drain its mailbox and a stalled queue must not stop the clock.
     */
    public long nextDeliveryNanos(int nodeId) {
        PriorityQueue<InFlight> mailbox = mailboxes.get(nodeId);
        InFlight head = mailbox == null ? null : mailbox.peek();
        return head == null ? Long.MAX_VALUE : head.dueNanos;
    }

    /**
     * Throws away everything queued for a node, as a crashed machine does.
     *
     * <p>Without this a restarted node would be handed a burst of datagrams from before it died,
     * which no real node ever sees: those packets were dropped by a host that was not listening.
     */
    public int discardPendingFor(int nodeId) {
        PriorityQueue<InFlight> mailbox = mailboxes.get(nodeId);
        if (mailbox == null) {
            return 0;
        }
        int discarded = mailbox.size();
        droppedByLoss += discarded;
        mailbox.clear();
        return discarded;
    }

    /** How many datagrams are queued but not yet delivered. */
    public int inFlight() {
        int total = 0;
        for (PriorityQueue<InFlight> mailbox : mailboxes.values()) {
            total += mailbox.size();
        }
        return total;
    }

    public long dropped() {
        return droppedByLoss + droppedByPartition;
    }

    private static long link(int from, int to) {
        return ((long) from << 32) | (to & 0xFFFFFFFFL);
    }

    private void enqueue(int from, int to, ByteBuffer datagram) {
        sent++;
        if (severed.contains(link(from, to))) {
            droppedByPartition++;
            return;
        }
        if (lossProbability > 0 && random.nextDouble() < lossProbability) {
            droppedByLoss++;
            return;
        }
        byte[] copy = new byte[datagram.remaining()];
        datagram.duplicate().get(copy);

        long delay = minDelayNanos;
        if (maxDelayNanos > minDelayNanos) {
            delay += (long) (random.nextDouble() * (maxDelayNanos - minDelayNanos));
        }
        PriorityQueue<InFlight> mailbox = mailboxes.computeIfAbsent(to, id -> new PriorityQueue<>());
        mailbox.add(new InFlight(clock.nanoTime() + delay, sequence++, from, copy));
    }

    private int drainDue(int nodeId, DatagramHandler handler) {
        PriorityQueue<InFlight> mailbox = mailboxes.get(nodeId);
        if (mailbox == null) {
            return 0;
        }
        long now = clock.nanoTime();
        List<InFlight> due = new ArrayList<>();
        while (!mailbox.isEmpty() && mailbox.peek().dueNanos <= now) {
            due.add(mailbox.poll());
        }
        for (InFlight message : due) {
            delivered++;
            handler.onDatagram(ByteBuffer.wrap(message.bytes));
        }
        return due.size();
    }

    /** One datagram waiting to be delivered. Ordered by due time, then by send order. */
    private static final class InFlight implements Comparable<InFlight> {

        private final long dueNanos;
        private final long sequence;
        private final int from;
        private final byte[] bytes;

        private InFlight(long dueNanos, long sequence, int from, byte[] bytes) {
            this.dueNanos = dueNanos;
            this.sequence = sequence;
            this.from = from;
            this.bytes = bytes;
        }

        @Override
        public int compareTo(InFlight other) {
            int byTime = Long.compare(dueNanos, other.dueNanos);
            return byTime != 0 ? byTime : Long.compare(sequence, other.sequence);
        }

        @Override
        public String toString() {
            return "from " + from + " due " + dueNanos + " (" + bytes.length + " bytes)";
        }
    }

    /** One node's view of the simulated network. */
    private final class SimulatedTransport implements Transport {

        private final int nodeId;
        private boolean closed;

        private SimulatedTransport(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public void send(int peerId, ByteBuffer datagram) {
            if (!closed) {
                enqueue(nodeId, peerId, datagram);
            }
        }

        @Override
        public int poll(long timeoutNanos, DatagramHandler handler) {
            return closed ? 0 : drainDue(nodeId, handler);
        }

        @Override
        public void wakeup() {
            // Nothing blocks here: the harness owns time, so poll always returns immediately.
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public String toString() {
            return "SimulatedTransport(" + nodeId + ")";
        }
    }

    @Override
    public String toString() {
        return "SimulatedNetwork(sent=" + sent + ", delivered=" + delivered
                + ", dropped=" + dropped() + ", inFlight=" + inFlight()
                + ", severed=" + severed.size() + " links)";
    }

    /** Convenience for the common two-group split. */
    public void partition(int[] sideA, int[] sideB) {
        List<Integer> a = new ArrayList<>();
        for (int id : sideA) {
            a.add(id);
        }
        List<Integer> b = new ArrayList<>();
        for (int id : sideB) {
            b.add(id);
        }
        partition(a, b);
    }

    /** Every node currently known to the network. */
    public Set<Integer> nodes() {
        return new HashSet<>(mailboxes.keySet());
    }
}
