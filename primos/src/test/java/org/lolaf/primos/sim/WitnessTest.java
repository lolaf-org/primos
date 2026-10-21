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

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Leader;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.Role;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two-node active/passive pair, which is what the witness exists for.
 *
 * <p>Two nodes alone tolerate zero failures: any death or partition leaves no leader at all. A
 * witness in a third failure domain makes it three voters with a quorum of two, so a partition has
 * a winner and a loser rather than two winners.
 */
class WitnessTest {

    private static final int WITNESS = ElectionCluster.WITNESS_ID;
    private static final Duration SETTLE = Duration.ofSeconds(5);

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }

    @Test
    void a_pair_split_from_each_other_is_decided_by_whoever_reaches_the_witness() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        // The nodes cannot see each other, but both can still see the witness. This is the exact
        // situation the witness was deployed for.
        simulation.network().sever(1, 2);
        simulation.network().sever(2, 1);
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.leaderCount())
                .as("one of them holds the cluster and the other stands down; never both")
                .isEqualTo(1);
        assertThat(cluster.leader().get().isWitness()).isFalse();
    }

    @Test
    void that_decision_is_stable_rather_than_flapping() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.network().sever(1, 2);
        simulation.network().sever(2, 1);
        simulation.run(Duration.ofSeconds(5));

        int leader = cluster.leader().get().id();
        long term = cluster.leader().get().currentTerm();
        simulation.run(Duration.ofSeconds(60));

        assertThat(cluster.leader().get().id()).isEqualTo(leader);
        assertThat(cluster.leader().get().currentTerm())
                .as("the witness can hear the leader, so it refuses the loser's pre-votes and no"
                        + " term is ever burned")
                .isEqualTo(term);
    }

    @Test
    void losing_one_node_leaves_the_other_leading() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int leader = cluster.leader().get().id();
        simulation.crash(leader);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(15)))
                .as("survivor plus witness is two of three, which is a quorum")
                .isTrue();
        assertThat(cluster.leader().get().id()).isNotEqualTo(leader);
    }

    @Test
    void losing_the_witness_leaves_the_pair_working() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int leader = cluster.leader().get().id();

        simulation.crash(WITNESS);
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.leaderCount())
                .as("both nodes are still up, and two of three is still a quorum")
                .isEqualTo(1);
        assertThat(cluster.leader().get().id()).isEqualTo(leader);
    }

    @Test
    void losing_the_witness_and_then_a_node_leaves_nobody_leading() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.crash(WITNESS);
        simulation.run(Duration.ofSeconds(5));
        simulation.crash(cluster.leader().get().id());
        simulation.run(Duration.ofSeconds(15));

        assertThat(cluster.leaderCount())
                .as("one voter of three is not a majority, however long it keeps asking")
                .isZero();
    }

    @Test
    void a_node_isolated_from_everything_stands_down_and_the_rest_carry_on() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int leader = cluster.leader().get().id();

        simulation.network().isolate(leader);
        simulation.run(Duration.ofSeconds(15));

        assertThat(cluster.node(leader).role()).isNotEqualTo(Role.LEADER);
        assertThat(cluster.leaderCount()).isEqualTo(1);
        assertThat(cluster.leader().get().id()).isNotEqualTo(leader);
    }

    @Test
    void every_node_isolated_from_every_other_leaves_nobody_leading() {
        Simulation simulation = simulation(7L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.network().isolate(1);
        simulation.network().isolate(2);
        simulation.network().isolate(WITNESS);
        simulation.run(Duration.ofSeconds(15));

        assertThat(cluster.leaderCount()).isZero();
    }

    @Test
    void healing_any_of_it_leaves_exactly_one_leader() {
        Simulation simulation = simulation(8L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.network().isolate(1);
        simulation.run(Duration.ofSeconds(10));
        simulation.network().heal();
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.leaderCount()).isEqualTo(1);
    }

    @Test
    void the_witness_never_appears_as_a_leader_to_anybody() {
        Simulation simulation = simulation(9L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        List<Integer> namedLeaders = new ArrayList<>();
        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            node.attach(new PrimosListener() {
                @Override
                public void onLeaderChanged(Optional<Leader> leader, long term) {
                    leader.map(Leader::id).ifPresent(namedLeaders::add);
                }
            });
        }

        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.network().isolate(1);
        simulation.run(Duration.ofSeconds(10));
        simulation.network().heal();
        simulation.run(Duration.ofSeconds(10));

        assertThat(namedLeaders)
                .as("work must never be forwarded to something that does not run the application")
                .isNotEmpty()
                .doesNotContain(WITNESS);
    }

    @Test
    void a_witness_that_crashes_and_returns_never_lets_two_leaders_exist() {
        Simulation simulation = simulation(10L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        for (int round = 0; round < 5; round++) {
            simulation.crash(WITNESS);
            simulation.run(Duration.ofSeconds(3));
            simulation.restart(WITNESS);
            simulation.run(Duration.ofSeconds(3));
        }

        assertThat(cluster.node(WITNESS).role()).isEqualTo(Role.FOLLOWER);
        assertThat(cluster.leaderCount()).isLessThanOrEqualTo(1);
    }

    @Test
    void a_witness_helps_a_four_node_cluster_too() {
        Simulation simulation = simulation(11L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 4, true);
        simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(15));

        simulation.network().partition(new int[]{1, 2}, new int[]{3, 4, WITNESS});
        simulation.run(Duration.ofSeconds(15));

        assertThat(cluster.leaderCount())
                .as("three of five is a quorum; two of five is not")
                .isEqualTo(1);
        assertThat(cluster.leader().get().id()).isIn(3, 4);
    }

    @Test
    void fifty_seeds_of_random_partitions_never_produce_two_leaders() {
        for (long seed = 1; seed <= 50; seed++) {
            Simulation simulation = simulation(seed);
            simulation.network().lossProbability(0.05);
            simulation.network().delayRange(0, TimeUnit.MILLISECONDS.toNanos(20));
            ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
            simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(20));

            for (int round = 0; round < 4; round++) {
                int victim = 1 + (simulation.random().nextInt() * 3);
                simulation.network().isolate(victim == 3 ? WITNESS : victim);
                simulation.run(Duration.ofSeconds(5));
                simulation.network().heal();
                simulation.run(Duration.ofSeconds(5));

                assertThat(cluster.leaderCount())
                        .as("seed %dL round %d ended with %d leaders", seed, round,
                                cluster.leaderCount())
                        .isLessThanOrEqualTo(1);
            }
        }
    }

    @Test
    void a_witness_is_reported_as_a_witness_not_as_an_application_peer() {
        Simulation simulation = simulation(12L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        List<Peer> seen = new ArrayList<>();
        cluster.node(1).attach(new PrimosListener() {
            @Override
            public void onPeerUp(Peer peer) {
                seen.add(peer);
            }
        });

        simulation.run(Duration.ofSeconds(10));

        assertThat(seen).extracting(Peer::id).contains(WITNESS);
        assertThat(seen).filteredOn(Peer::isWitness).extracting(Peer::id)
                .containsExactly(WITNESS);
    }
}
