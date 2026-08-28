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
package org.lolaf.primos.sim;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;

import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.SimulatedNetwork;

/**
 * Runs a whole cluster on one thread over a virtual clock.
 *
 * <p>This is the primary test harness, and the reason the algorithm is written after it rather
 * than before. Wall-clock multi-threaded tests find the races that happen to surface on the
 * machine they ran on; a seeded simulation that partitions, delays and crashes on demand finds the
 * ones that matter, finds them the same way twice, and turns each one into a regression by
 * checking in its seed.
 *
 * <p>Time moves in jumps, not ticks: the scheduler advances straight to the earliest of the next
 * queued datagram and the next node deadline. A partition lasting four election timeouts therefore
 * costs no wall-clock time, and a run covering an hour of cluster life finishes in milliseconds.
 *
 * <p>Everything is deterministic given the seed. Nodes are ticked in id order, the network delivers
 * in a defined order, and the same seed produces a byte-identical trace — which is what makes a
 * checked-in regression seed worth anything.
 */
public final class Simulation {

    private static final int TRACE_TAIL = 40;

    private final long seed;
    private final Random random;
    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network;
    private final List<SimNode> nodes = new ArrayList<>();
    private final List<Invariant> invariants = new ArrayList<>();
    private final Deque<String> trace = new ArrayDeque<>();
    private final StringBuilder digest = new StringBuilder();

    private long steps;

    public Simulation(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        this.network = new SimulatedNetwork(seed, clock);
    }

    /** The seed this run was built from. Quote it in any failure. */
    public long seed() {
        return seed;
    }

    public VirtualClock clock() {
        return clock;
    }

    public SimulatedNetwork network() {
        return network;
    }

    /** A source of randomness that is part of the seeded run, for tests injecting their own faults. */
    public Random random() {
        return random;
    }

    /** The cluster, in id order. */
    public List<SimNode> nodes() {
        return Collections.unmodifiableList(nodes);
    }

    /** The node with an id, or null. */
    public SimNode node(int id) {
        for (SimNode node : nodes) {
            if (node.id() == id) {
                return node;
            }
        }
        return null;
    }

    /** Adds a node. Nodes are kept in id order so the tick order never depends on insertion order. */
    public Simulation add(SimNode node) {
        nodes.add(node);
        nodes.sort(Comparator.comparingInt(SimNode::id));
        return this;
    }

    /** Attaches invariants, checked after every step. */
    public Simulation checking(Invariant... toCheck) {
        Collections.addAll(invariants, toCheck);
        return this;
    }

    /** How many scheduler steps have run. */
    public long steps() {
        return steps;
    }

    /**
     * Runs one step: jump to the next thing that happens, let every running node act, then check
     * every invariant.
     *
     * @return false if nothing is left to do, so the caller can stop early
     */
    public boolean step() {
        long next = nextEventNanos();
        if (next == Long.MAX_VALUE) {
            return false;
        }
        clock.advanceTo(Math.max(next, clock.nanoTime()));
        tickAll();
        return true;
    }

    /** Runs until the given amount of simulated time has passed. */
    public void run(Duration duration) {
        long deadline = clock.nanoTime() + duration.toNanos();
        while (clock.nanoTime() < deadline) {
            long next = Math.min(nextEventNanos(), deadline);
            clock.advanceTo(Math.max(next, clock.nanoTime()));
            tickAll();
        }
    }

    /**
     * Runs until a condition holds or the time limit passes.
     *
     * @return whether the condition held
     */
    public boolean runUntil(BooleanSupplier condition, Duration limit) {
        long deadline = clock.nanoTime() + limit.toNanos();
        while (clock.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            long next = Math.min(nextEventNanos(), deadline);
            clock.advanceTo(Math.max(next, clock.nanoTime()));
            tickAll();
        }
        return condition.getAsBoolean();
    }

    /** Crashes a node, discarding everything it had not made durable. */
    public void crash(int nodeId) {
        SimNode node = requireNode(nodeId);
        node.crash();
        record("crash n" + nodeId);
    }

    /** Restarts a crashed node, which reads its state back from the store that outlived it. */
    public void restart(int nodeId) {
        SimNode node = requireNode(nodeId);
        node.restart();
        record("restart n" + nodeId);
    }

    /**
     * A fingerprint of everything that happened, byte-identical across runs of the same seed.
     *
     * <p>Two simulations built from one seed must produce the same string. If they do not, some
     * hidden source of nondeterminism — a hash iteration order, a wall-clock read, a thread — has
     * crept in, and every regression seed in the suite has quietly stopped meaning anything.
     */
    public String traceDigest() {
        return digest.toString();
    }

    /** The last few events, for a failure message. */
    public String recentTrace() {
        StringBuilder out = new StringBuilder();
        for (String line : trace) {
            out.append("    ").append(line).append('\n');
        }
        return out.toString();
    }

    private void tickAll() {
        for (SimNode node : nodes) {
            if (node.isRunning()) {
                node.tick();
            }
        }
        steps++;
        snapshot();
        checkInvariants();
    }

    private long nextEventNanos() {
        long earliest = network.nextDeliveryNanos();
        for (SimNode node : nodes) {
            if (node.isRunning()) {
                earliest = Math.min(earliest, node.nextDeadlineNanos());
            }
        }
        return earliest;
    }

    private void snapshot() {
        StringBuilder line = new StringBuilder();
        line.append('t').append(clock.nanoTime());
        for (SimNode node : nodes) {
            line.append(" n").append(node.id()).append(':');
            if (!node.isRunning()) {
                line.append("down");
            } else {
                line.append(node.role()).append('/').append(node.currentTerm())
                        .append('/').append(node.votedFor());
            }
        }
        line.append(" net=").append(network.delivered()).append('/').append(network.dropped());
        record(line.toString());
    }

    private void record(String event) {
        digest.append(event).append('\n');
        trace.addLast(event);
        while (trace.size() > TRACE_TAIL) {
            trace.removeFirst();
        }
    }

    private void checkInvariants() {
        for (Invariant invariant : invariants) {
            try {
                invariant.check(this);
            } catch (AssertionError e) {
                throw new SimulationFailure(seed, clock.nanoTime(), invariant.name(),
                        e.getMessage(), recentTrace());
            }
        }
    }

    private SimNode requireNode(int nodeId) {
        SimNode node = node(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("no node " + nodeId + " in this simulation");
        }
        return node;
    }
}
