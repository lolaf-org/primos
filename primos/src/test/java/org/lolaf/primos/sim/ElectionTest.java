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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Role;

/**
 * Leader election, driven entirely by the simulation.
 *
 * <p>Every safety invariant is attached to every run here, so a scenario aimed at one property
 * also checks the rest continuously.
 */
class ElectionTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);

    @Test
    void a_three_node_cluster_elects_exactly_one_leader() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE)).isTrue();
        assertThat(cluster.leaderCount()).isEqualTo(1);
        assertThat(cluster.leader()).isPresent();
    }

    @Test
    void a_single_node_cluster_leads_itself_without_asking_anyone() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 1, false);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE)).isTrue();
        assertThat(cluster.node(1).role()).isEqualTo(Role.LEADER);
        assertThat(cluster.node(1).currentTerm()).isEqualTo(1);
    }

    @Test
    void a_two_node_pair_with_a_witness_elects_a_leader() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE)).isTrue();
        assertThat(cluster.leader().get().isWitness()).isFalse();
    }

    @Test
    void a_witness_votes_but_never_leads() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);

        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.run(Duration.ofSeconds(3));

        ElectionCluster.NodeUnderTest witness = cluster.node(ElectionCluster.WITNESS_ID);
        assertThat(witness.role()).isEqualTo(Role.FOLLOWER);
        assertThat(witness.votedFor()).isNotNegative();
        assertThat(witness.nextDeadlineNanos())
                .as("a witness has no election timer, so it waits only on the network")
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void a_two_node_pair_without_a_witness_cannot_elect_anyone_once_one_dies() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int leader = cluster.leader().get().id();
        simulation.crash(leader);
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leaderCount())
                .as("two voters need two votes, so one survivor is not a quorum")
                .isZero();
    }

    @Test
    void the_survivors_replace_a_leader_that_dies() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int firstLeader = cluster.leader().get().id();
        long termBefore = cluster.leader().get().currentTerm();
        simulation.crash(firstLeader);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE)).isTrue();
        assertThat(cluster.leader().get().id()).isNotEqualTo(firstLeader);
        assertThat(cluster.leader().get().currentTerm()).isGreaterThan(termBefore);
    }

    @Test
    void a_leader_that_comes_back_accepts_the_new_one() {
        Simulation simulation = simulation(7L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int firstLeader = cluster.leader().get().id();
        simulation.crash(firstLeader);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.restart(firstLeader);
        simulation.run(Duration.ofSeconds(3));

        assertThat(cluster.leaderCount()).isEqualTo(1);
        assertThat(cluster.node(firstLeader).role()).isNotEqualTo(Role.LEADER);
    }

    @Test
    void only_the_majority_side_of_a_partition_has_a_leader() {
        Simulation simulation = simulation(8L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.network().partition(new int[] {1, 2}, new int[] {3, 4, 5});
        simulation.run(Duration.ofSeconds(5));

        long majorityLeaders = cluster.nodes().stream()
                .filter(node -> node.id() >= 3 && node.role() == Role.LEADER).count();
        long minorityLeaders = cluster.nodes().stream()
                .filter(node -> node.id() <= 2 && node.role() == Role.LEADER).count();

        assertThat(majorityLeaders).isEqualTo(1);
        assertThat(minorityLeaders)
                .as("two of five is not a quorum, however long they keep trying")
                .isZero();
    }

    @Test
    void a_healed_partition_leaves_exactly_one_leader() {
        Simulation simulation = simulation(9L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.network().partition(new int[] {1, 2}, new int[] {3, 4, 5});
        simulation.run(Duration.ofSeconds(5));
        simulation.network().heal();
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leaderCount()).isEqualTo(1);
    }

    @Test
    void an_isolated_pair_rejoining_does_not_produce_a_second_leader() {
        Simulation simulation = simulation(10L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.network().partition(new int[] {4, 5}, new int[] {1, 2, 3});
        simulation.run(Duration.ofSeconds(10));
        simulation.network().heal();
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.leaderCount()).isEqualTo(1);
    }

    @Test
    void a_leader_emerges_even_when_a_fifth_of_datagrams_are_lost() {
        Simulation simulation = simulation(11L);
        simulation.network().lossProbability(0.2);
        simulation.network().delayRange(0, TimeUnit.MILLISECONDS.toNanos(20));
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(30))).isTrue();
    }

    @Test
    void randomised_timeouts_stop_nodes_campaigning_in_lockstep() {
        Simulation simulation = simulation(12L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);

        long distinct = cluster.nodes().stream()
                .mapToLong(ElectionCluster.NodeUnderTest::nextDeadlineNanos)
                .distinct().count();

        assertThat(distinct)
                .as("nodes that time out together campaign together and split the vote forever")
                .isGreaterThan(1);
    }

    @Test
    void a_hundred_seeds_all_settle_on_exactly_one_leader() {
        for (long seed = 1; seed <= 100; seed++) {
            Simulation simulation = simulation(seed);
            simulation.network().lossProbability(0.1);
            simulation.network().delayRange(0, TimeUnit.MILLISECONDS.toNanos(15));
            ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);

            boolean settled = simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(30));

            assertThat(settled)
                    .as("seed %dL produced no leader within 30s", seed)
                    .isTrue();
        }
    }

    @Test
    void a_cluster_survives_repeated_crashes_of_whoever_is_leading() {
        Simulation simulation = simulation(13L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);

        for (int round = 0; round < 10; round++) {
            assertThat(simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(30)))
                    .as("round %d found no leader", round)
                    .isTrue();
            int leader = cluster.leader().get().id();
            simulation.crash(leader);
            simulation.run(Duration.ofMillis(200));
            simulation.restart(leader);
        }
    }

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }
}
