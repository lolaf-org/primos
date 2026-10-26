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
import org.lolaf.primos.RecordingStats;
import org.lolaf.primos.Role;
import org.lolaf.primos.Timings;

/**
 * A leader handing leadership over without leaving: the stop handover's {@code TimeoutNow}, asked
 * for while the node stays in the cluster.
 */
class TransferLeadershipTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);
    private static final Timings TIMINGS = Timings.lan();

    @Test
    void a_transfer_elects_a_successor_well_inside_an_election_timeout_and_the_old_leader_follows() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        RecordingStats recorded = new RecordingStats();
        cluster.nodes().forEach(node -> node.attach(recorded));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest old = cluster.leader().get();
        long term = old.currentTerm();

        assertThat(old.election().transferLeadership()).isTrue();

        assertThat(simulation.runUntil(() -> cluster.hasSingleLeader() && cluster.leader().get().id() != old.id(),
                TIMINGS.electionTimeoutMin().dividedBy(2)))
                .as("a round trip, not the election timeout a crash costs")
                .isTrue();
        assertThat(cluster.leader().get().currentTerm()).isEqualTo(term + 1);
        assertThat(old.isRunning()).as("a transfer is not a stop").isTrue();
        assertThat(old.role()).isEqualTo(Role.FOLLOWER);
        assertThat(recorded.transfersSent.get()).isEqualTo(1);
        assertThat(recorded.handoversSent.get()).as("counted apart from a shutdown's handover").isZero();
        assertThat(recorded.handoversAccepted.get()).isEqualTo(1);
    }

    @Test
    void a_transfer_to_a_chosen_follower_elects_that_follower() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest old = cluster.leader().get();
        int chosen = cluster.nodes().stream().mapToInt(ElectionCluster.NodeUnderTest::id)
                .filter(id -> id != old.id()).max().getAsInt();

        assertThat(old.election().transferLeadership(chosen)).isTrue();

        assertThat(simulation.runUntil(() -> cluster.hasSingleLeader() && cluster.leader().get().id() == chosen,
                TIMINGS.electionTimeoutMin().dividedBy(2))).isTrue();
    }

    @Test
    void only_a_leader_transfers() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int leader = cluster.leader().get().id();
        ElectionCluster.NodeUnderTest follower = follower(cluster);

        assertThat(follower.election().transferLeadership()).isFalse();
        assertThat(follower.election().transferLeadership(leader)).isFalse();
    }

    @Test
    void a_witness_an_unknown_id_or_the_leader_itself_is_refused_as_target() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        long term = leader.currentTerm();

        assertThat(leader.election().transferLeadership(ElectionCluster.WITNESS_ID)).isFalse();
        assertThat(leader.election().transferLeadership(999)).isFalse();
        assertThat(leader.election().transferLeadership(leader.id())).isFalse();
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leader().get().id()).isEqualTo(leader.id());
        assertThat(leader.currentTerm()).isEqualTo(term);
    }

    @Test
    void a_peer_declared_down_is_refused_as_target() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        ElectionCluster.NodeUnderTest gone = follower(cluster);

        // A peer never yet probed reads as unreachable too, so it must be seen up before it can be seen go.
        assertThat(simulation.runUntil(() -> leader.election().isReachable(gone.id()), Duration.ofSeconds(60))).isTrue();
        simulation.network().isolate(gone.id());
        assertThat(simulation.runUntil(() -> !leader.election().isReachable(gone.id()), Duration.ofSeconds(60))).isTrue();

        assertThat(leader.election().transferLeadership(gone.id())).isFalse();
    }

    @Test
    void a_lost_transfer_changes_nothing() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        long term = leader.currentTerm();
        ElectionCluster.NodeUnderTest target = follower(cluster);

        simulation.network().sever(leader.id(), target.id());
        assertThat(leader.election().transferLeadership(target.id())).as("sent, and dropped").isTrue();
        simulation.network().restore(leader.id(), target.id());
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leader().get().id())
                .as("the leader keeps leading until a successor campaigns, so a lost TimeoutNow costs nothing")
                .isEqualTo(leader.id());
        assertThat(leader.currentTerm()).isEqualTo(term);
    }

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }

    private static ElectionCluster.NodeUnderTest follower(ElectionCluster cluster) {
        return cluster.nodes().stream()
                .filter(node -> node.role() == Role.FOLLOWER && !node.isWitness())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no follower"));
    }
}
