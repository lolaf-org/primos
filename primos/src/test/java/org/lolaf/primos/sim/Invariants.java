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

import java.util.HashMap;
import java.util.Map;

import org.lolaf.primos.Role;

/**
 * The safety properties primos must never violate.
 *
 * <p>These are the whole point of the simulation. The algorithm can be read and believed; only
 * these, asserted continuously across thousands of seeded runs full of partitions and crashes,
 * make it trustworthy.
 */
public final class Invariants {

    private Invariants() {
    }

    /** Every invariant, ready to attach to a simulation. */
    public static Invariant[] all() {
        return new Invariant[] {
                atMostOneLeaderPerTerm(),
                termNeverDecreases(),
                atMostOneVotePerTerm(),
                witnessNeverLeads(),
        };
    }

    /**
     * The central safety property of Raft. Two leaders in one term means two nodes each believed
     * they had a majority, which means a vote was counted twice or granted twice.
     */
    public static Invariant atMostOneLeaderPerTerm() {
        return new Invariant() {

            private final Map<Long, Integer> leaderByTerm = new HashMap<>();

            @Override
            public String name() {
                return "at most one leader per term";
            }

            @Override
            public void check(Simulation simulation) {
                for (SimNode node : simulation.nodes()) {
                    if (!node.isRunning() || node.role() != Role.LEADER) {
                        continue;
                    }
                    Integer existing = leaderByTerm.putIfAbsent(node.currentTerm(), node.id());
                    if (existing != null && existing != node.id()) {
                        throw new AssertionError("node " + existing + " and node " + node.id()
                                + " were both leader in term " + node.currentTerm());
                    }
                }
            }
        };
    }

    /**
     * A term that went backwards means a node forgot something it had promised, which is the
     * signature of state that was acted on before it was made durable.
     */
    public static Invariant termNeverDecreases() {
        return new Invariant() {

            private final Map<Integer, Long> highestTerm = new HashMap<>();

            @Override
            public String name() {
                return "currentTerm never decreases at a node";
            }

            @Override
            public void check(Simulation simulation) {
                for (SimNode node : simulation.nodes()) {
                    if (!node.isRunning()) {
                        continue;
                    }
                    Long previous = highestTerm.get(node.id());
                    if (previous != null && node.currentTerm() < previous) {
                        throw new AssertionError("node " + node.id() + " went from term " + previous
                                + " back to term " + node.currentTerm()
                                + ", so it acted on state it had not made durable");
                    }
                    highestTerm.put(node.id(), node.currentTerm());
                }
            }
        };
    }

    /**
     * One vote per term, remembered across a crash.
     *
     * <p>This is the invariant a missing fsync breaks. A node that grants a vote, dies before
     * persisting it and comes back will happily grant a second one in the same term — and the two
     * candidates each collect a majority. The check deliberately keeps its record across crashes,
     * because that is exactly the window being tested.
     */
    public static Invariant atMostOneVotePerTerm() {
        return new Invariant() {

            private final Map<Integer, Long> voteTerm = new HashMap<>();
            private final Map<Integer, Integer> voteFor = new HashMap<>();

            @Override
            public String name() {
                return "a node votes at most once per term, across crashes";
            }

            @Override
            public void check(Simulation simulation) {
                for (SimNode node : simulation.nodes()) {
                    if (!node.isRunning() || node.votedFor() < 0) {
                        continue;
                    }
                    Long knownTerm = voteTerm.get(node.id());
                    if (knownTerm != null && knownTerm == node.currentTerm()) {
                        int knownVote = voteFor.get(node.id());
                        if (knownVote != node.votedFor()) {
                            throw new AssertionError("node " + node.id() + " voted for "
                                    + knownVote + " and then for " + node.votedFor()
                                    + " in the same term " + node.currentTerm()
                                    + ", which lets two candidates each win a majority");
                        }
                    } else if (knownTerm == null || node.currentTerm() > knownTerm) {
                        voteTerm.put(node.id(), node.currentTerm());
                        voteFor.put(node.id(), node.votedFor());
                    }
                }
            }
        };
    }

    /** A witness votes, but it must never campaign and never lead. */
    public static Invariant witnessNeverLeads() {
        return new Invariant() {

            @Override
            public String name() {
                return "a witness never becomes candidate or leader";
            }

            @Override
            public void check(Simulation simulation) {
                for (SimNode node : simulation.nodes()) {
                    if (node.isWitness() && node.isRunning()
                            && (node.role() == Role.LEADER || node.role() == Role.CANDIDATE)) {
                        throw new AssertionError("witness " + node.id() + " reached role "
                                + node.role() + " in term " + node.currentTerm());
                    }
                }
            }
        };
    }
}
