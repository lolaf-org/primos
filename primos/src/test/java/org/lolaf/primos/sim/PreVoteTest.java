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

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Role;

/**
 * A node that has been away must not disturb the cluster it comes back to.
 *
 * <p>Without pre-voting, a partitioned node keeps timing out and incrementing its term, and the
 * moment the partition heals it forces a healthy leader to step down. For a two-node
 * active/passive pair that is a failover nobody asked for, caused entirely by a node that was
 * never needed.
 */
class PreVoteTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);

    @Test
    void a_partitioned_node_never_advances_its_term() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        ElectionCluster.NodeUnderTest stranded = follower(cluster);
        long termWhenIsolated = stranded.currentTerm();

        simulation.network().isolate(stranded.id());
        simulation.run(Duration.ofSeconds(60));

        assertThat(stranded.currentTerm())
                .as("sixty seconds of timing out, and not one wasted term")
                .isEqualTo(termWhenIsolated);
        assertThat(stranded.election().preVotesStarted())
                .as("it kept asking, it just never got a yes")
                .isPositive();
        assertThat(stranded.election().electionsStarted()).isZero();
    }

    @Test
    void a_returning_node_does_not_displace_the_leader() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int leaderBefore = cluster.leader().get().id();
        long termBefore = cluster.leader().get().currentTerm();
        ElectionCluster.NodeUnderTest stranded = follower(cluster);

        simulation.network().isolate(stranded.id());
        simulation.run(Duration.ofSeconds(30));
        simulation.network().heal();
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leader().get().id())
                .as("the same node still leads")
                .isEqualTo(leaderBefore);
        assertThat(cluster.leader().get().currentTerm())
                .as("and in the same term: no election happened at all")
                .isEqualTo(termBefore);
    }

    @Test
    void a_node_crashing_and_restarting_does_not_displace_the_leader_either() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        long termBefore = cluster.leader().get().currentTerm();
        ElectionCluster.NodeUnderTest victim = follower(cluster);

        simulation.crash(victim.id());
        simulation.run(Duration.ofSeconds(10));
        simulation.restart(victim.id());
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leaderCount()).isEqualTo(1);
        assertThat(cluster.leader().get().currentTerm()).isEqualTo(termBefore);
    }

    @Test
    void a_cold_cluster_still_elects_a_leader() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE))
                .as("pre-voting must not deadlock a cluster where nobody has heard from anyone")
                .isTrue();
    }

    @Test
    void a_dead_leader_is_still_replaced_promptly() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int leader = cluster.leader().get().id();
        simulation.crash(leader);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(10)))
                .as("refusing pre-votes while a leader is alive must not refuse them once it is not")
                .isTrue();
        assertThat(cluster.leader().get().id()).isNotEqualTo(leader);
    }

    @Test
    void a_minority_side_of_a_partition_burns_no_terms() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int leader = cluster.leader().get().id();
        int[] minority = cluster.nodes().stream()
                .filter(n -> n.id() != leader)
                .mapToInt(ElectionCluster.NodeUnderTest::id)
                .limit(2).toArray();
        int[] majority = cluster.nodes().stream()
                .filter(n -> n.id() != minority[0] && n.id() != minority[1])
                .mapToInt(ElectionCluster.NodeUnderTest::id).toArray();

        long termBefore = cluster.node(minority[0]).currentTerm();
        simulation.network().partition(minority, majority);
        simulation.run(Duration.ofSeconds(30));

        assertThat(cluster.node(minority[0]).currentTerm()).isEqualTo(termBefore);
        assertThat(cluster.node(minority[1]).currentTerm()).isEqualTo(termBefore);
    }

    @Test
    void the_witness_side_of_a_pair_still_fails_over_when_the_leader_dies() {
        Simulation simulation = simulation(7L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int leader = cluster.leader().get().id();
        simulation.crash(leader);

        assertThat(simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(10))).isTrue();
        assertThat(cluster.leader().get().id()).isNotEqualTo(leader);
        assertThat(cluster.leader().get().isWitness()).isFalse();
    }

    @Test
    void thirty_seeds_of_isolate_and_heal_never_change_the_leader() {
        int leaderChanges = 0;
        for (long seed = 1; seed <= 30; seed++) {
            Simulation simulation = simulation(seed);
            ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
            if (!simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(20))) {
                continue;
            }
            int before = cluster.leader().get().id();
            ElectionCluster.NodeUnderTest stranded = follower(cluster);

            simulation.network().isolate(stranded.id());
            simulation.run(Duration.ofSeconds(20));
            simulation.network().heal();
            simulation.run(Duration.ofSeconds(5));

            if (!cluster.hasSingleLeader() || cluster.leader().get().id() != before) {
                leaderChanges++;
            }
        }

        assertThat(leaderChanges)
                .as("an absent follower is never a reason to hold an election")
                .isZero();
    }

    private static ElectionCluster.NodeUnderTest follower(ElectionCluster cluster) {
        return cluster.nodes().stream()
                .filter(node -> node.role() != Role.LEADER && !node.isWitness())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no follower"));
    }

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }
}
