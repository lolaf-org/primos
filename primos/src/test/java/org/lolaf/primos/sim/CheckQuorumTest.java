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
import org.lolaf.primos.Timings;

/**
 * A leader that cannot reach a majority must stop saying it is the leader.
 *
 * <p>This is the difference between primos being safe to build an active/passive pair on and being
 * a way to get two active nodes. Plain Raft does not need it, because a partitioned leader there
 * can commit nothing; primos has no log, so the claim itself is the entire output.
 */
class CheckQuorumTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);

    @Test
    void an_isolated_leader_stands_down() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int leader = cluster.leader().get().id();

        simulation.network().isolate(leader);
        simulation.run(Duration.ofSeconds(3));

        assertThat(cluster.node(leader).role())
                .as("it can reach nobody, so it must not keep claiming the cluster")
                .isNotEqualTo(Role.LEADER);
    }

    @Test
    void it_stands_down_within_an_election_timeout_of_losing_contact() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();

        simulation.network().isolate(leader.id());
        long isolatedAt = simulation.clock().nanoTime();
        simulation.runUntil(() -> leader.role() != Role.LEADER, Duration.ofSeconds(5));
        long stoodDownAfter = simulation.clock().nanoTime() - isolatedAt;

        assertThat(leader.role()).isNotEqualTo(Role.LEADER);
        assertThat(Duration.ofNanos(stoodDownAfter))
                .as("the window is the minimum election timeout plus a heartbeat to notice")
                .isLessThan(Duration.ofMillis(1_000));
    }

    @Test
    void a_leader_that_still_reaches_a_majority_keeps_leading() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();

        int firstVictim = otherThan(cluster, leader.id(), -1);
        int secondVictim = otherThan(cluster, leader.id(), firstVictim);
        simulation.network().partition(new int[] {firstVictim, secondVictim},
                new int[] {leader.id()});
        simulation.run(Duration.ofSeconds(3));

        assertThat(leader.role())
                .as("three of five is still a quorum, so nothing has gone wrong")
                .isEqualTo(Role.LEADER);
    }

    @Test
    void a_one_way_partition_that_swallows_its_heartbeats_still_stands_it_down() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();

        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            if (node.id() != leader.id()) {
                simulation.network().sever(leader.id(), node.id());
            }
        }
        simulation.run(Duration.ofSeconds(3));

        assertThat(leader.role())
                .as("it can still hear them, but nothing it sends arrives, so it leads nobody")
                .isNotEqualTo(Role.LEADER);
    }

    @Test
    void a_single_node_cluster_never_stands_itself_down() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 1, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.node(1).role())
                .as("one voter is its own majority, so there is no contact to lose")
                .isEqualTo(Role.LEADER);
        assertThat(cluster.node(1).election().leadershipsRelinquished()).isZero();
    }

    @Test
    void a_pair_keeps_its_leader_while_it_can_reach_the_witness() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        int other = leader.id() == 1 ? 2 : 1;

        simulation.network().partition(new int[] {leader.id()}, new int[] {other});
        simulation.run(Duration.ofSeconds(3));

        assertThat(leader.role())
                .as("leader plus witness is two of three, which is exactly what the witness is for")
                .isEqualTo(Role.LEADER);
    }

    @Test
    void losing_the_witness_as_well_stands_the_leader_down() {
        Simulation simulation = simulation(7L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();

        simulation.network().isolate(leader.id());
        simulation.run(Duration.ofSeconds(3));

        assertThat(leader.role()).isNotEqualTo(Role.LEADER);
    }

    @Test
    void the_old_leader_gives_up_before_the_cluster_settles_on_a_new_one() {
        Simulation simulation = simulation(8L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest old = cluster.leader().get();

        simulation.network().partition(new int[] {old.id()},
                cluster.nodes().stream().filter(n -> n.id() != old.id())
                        .mapToInt(ElectionCluster.NodeUnderTest::id).toArray());
        simulation.run(Duration.ofSeconds(5));

        assertThat(old.role()).isNotEqualTo(Role.LEADER);
        assertThat(cluster.leaderCount())
                .as("exactly one node claims the cluster, and it is not the isolated one")
                .isEqualTo(1);
    }

    @Test
    void a_leader_rejoining_after_standing_down_does_not_displace_the_new_one() {
        Simulation simulation = simulation(9L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int old = cluster.leader().get().id();

        simulation.network().isolate(old);
        simulation.run(Duration.ofSeconds(5));
        simulation.network().heal();
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leaderCount()).isEqualTo(1);
    }

    @Test
    void fifty_seeds_of_partitions_never_leave_a_leader_stranded() {
        for (long seed = 1; seed <= 50; seed++) {
            Simulation simulation = simulation(seed);
            simulation.network().delayRange(0, TimeUnit.MILLISECONDS.toNanos(10));
            ElectionCluster cluster = ElectionCluster.of(simulation, 3, false,
                    Timings.lan());

            simulation.runUntil(cluster::hasSingleLeader, Duration.ofSeconds(20));
            int leader = cluster.leader().map(ElectionCluster.NodeUnderTest::id).orElse(1);

            simulation.network().isolate(leader);
            simulation.run(Duration.ofSeconds(6));
            simulation.network().heal();
            simulation.run(Duration.ofSeconds(6));

            assertThat(cluster.leaderCount())
                    .as("seed %dL ended with %d leaders", seed, cluster.leaderCount())
                    .isEqualTo(1);
        }
    }

    private static int otherThan(ElectionCluster cluster, int a, int b) {
        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            if (node.id() != a && node.id() != b) {
                return node.id();
            }
        }
        throw new IllegalStateException("no third node");
    }

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }
}
