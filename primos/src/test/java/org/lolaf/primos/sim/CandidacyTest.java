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
import org.lolaf.primos.Timings;

/**
 * A node withdrawing from elections at runtime: the witness's rules, switched on and off.
 */
class CandidacyTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);
    private static final Timings TIMINGS = Timings.lan();

    @Test
    void a_node_that_withdrew_never_campaigns_even_when_the_leader_is_gone() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int leader = cluster.leader().get().id();
        ElectionCluster.NodeUnderTest withdrawn = followers(cluster)[0];
        ElectionCluster.NodeUnderTest standing = followers(cluster)[1];

        withdrawn.election().setCandidate(false);
        cluster.node(leader).crash();
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.leader()).hasValueSatisfying(node -> assertThat(node.id()).isEqualTo(standing.id()));
        assertThat(withdrawn.election().isCandidate()).isFalse();
        assertThat(withdrawn.election().preVotesStarted()).as("no timer, so not even a pre-vote").isZero();
    }

    @Test
    void the_node_still_votes_so_the_quorum_it_belongs_to_still_forms() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int leader = cluster.leader().get().id();
        followers(cluster)[0].election().setCandidate(false);

        cluster.node(leader).crash();

        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE))
                .as("two of three alive, one of them withdrawn: its vote is what makes the majority")
                .isTrue();
    }

    @Test
    void a_withdrawn_node_ignores_a_handover() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        long term = leader.currentTerm();
        ElectionCluster.NodeUnderTest withdrawn = followers(cluster)[0];
        withdrawn.election().setCandidate(false);

        // A leader that has not learned of the withdrawal can still pick the node; it says no.
        assertThat(leader.election().transferLeadership(withdrawn.id())).isTrue();
        simulation.run(Duration.ofSeconds(5));

        assertThat(withdrawn.election().handoversAccepted()).isZero();
        assertThat(cluster.leader().get().id()).isEqualTo(leader.id());
        assertThat(leader.currentTerm()).isEqualTo(term);
    }

    @Test
    void a_leader_that_withdraws_hands_over_and_follows() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest old = cluster.leader().get();

        old.election().setCandidate(false);

        assertThat(simulation.runUntil(() -> cluster.hasSingleLeader() && cluster.leader().get().id() != old.id(),
                TIMINGS.electionTimeoutMin().dividedBy(2))).isTrue();
        simulation.run(Duration.ofSeconds(10));
        assertThat(old.role()).as("and never takes it back").isEqualTo(Role.FOLLOWER);
    }

    @Test
    void a_leader_that_withdraws_keeps_leading_while_nobody_will_take_over() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest old = cluster.leader().get();
        ElectionCluster.NodeUnderTest[] followers = followers(cluster);
        followers[0].election().setCandidate(false);
        followers[1].election().setCandidate(false);

        old.election().setCandidate(false);
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leader().get().id()).as("nobody would take it, and leaving would leave no leader")
                .isEqualTo(old.id());
        assertThat(old.role()).isEqualTo(Role.LEADER);
    }

    @Test
    void a_withdrawn_leader_hands_over_to_the_follower_that_still_stands() {
        Simulation simulation = simulation(8L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest old = cluster.leader().get();
        ElectionCluster.NodeUnderTest[] followers = followers(cluster);
        for (int i = 0; i < followers.length - 1; i++) {
            followers[i].election().setCandidate(false);
        }
        ElectionCluster.NodeUnderTest standing = followers[followers.length - 1];
        // Long enough for every follower's next heartbeat reply to have said so.
        simulation.run(TIMINGS.heartbeatInterval().multipliedBy(3));

        old.election().setCandidate(false);

        assertThat(simulation.runUntil(() -> cluster.hasSingleLeader() && cluster.leader().get().id() == standing.id(),
                TIMINGS.electionTimeoutMin().dividedBy(2)))
                .as("the leader knows who withdrew, so the first offer goes to the one that stands")
                .isTrue();
    }

    @Test
    void a_withdrawn_leader_hands_over_as_soon_as_a_follower_stands_again() {
        Simulation simulation = simulation(9L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest old = cluster.leader().get();
        ElectionCluster.NodeUnderTest[] followers = followers(cluster);
        followers[0].election().setCandidate(false);
        followers[1].election().setCandidate(false);
        old.election().setCandidate(false);
        simulation.run(Duration.ofSeconds(5));

        followers[1].election().setCandidate(true);

        assertThat(simulation.runUntil(() -> cluster.hasSingleLeader() && cluster.leader().get().id() == followers[1].id(),
                SETTLE)).as("the withdrawn leader kept offering").isTrue();
    }

    @Test
    void a_follower_that_withdrew_is_refused_as_a_chosen_target() {
        Simulation simulation = simulation(10L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        ElectionCluster.NodeUnderTest withdrawn = followers(cluster)[0];
        withdrawn.election().setCandidate(false);
        simulation.run(TIMINGS.heartbeatInterval().multipliedBy(3));

        assertThat(leader.election().transferLeadership(withdrawn.id())).isFalse();
    }

    @Test
    void a_candidate_withdrawing_mid_campaign_does_not_lead() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        cluster.node(cluster.leader().get().id()).crash();
        assertThat(simulation.runUntil(() -> cluster.nodes().stream().anyMatch(node -> node.role() == Role.CANDIDATE),
                SETTLE)).isTrue();
        ElectionCluster.NodeUnderTest campaigning = cluster.nodes().stream()
                .filter(node -> node.role() == Role.CANDIDATE).findFirst().get();

        campaigning.election().setCandidate(false);
        simulation.run(Duration.ofSeconds(10));

        assertThat(campaigning.role()).isEqualTo(Role.FOLLOWER);
        assertThat(cluster.leader()).hasValueSatisfying(node -> assertThat(node.id()).isNotEqualTo(campaigning.id()));
    }

    @Test
    void a_node_standing_again_campaigns_after_a_full_timeout_not_at_once() {
        Simulation simulation = simulation(7L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest follower = followers(cluster)[0];
        follower.election().setCandidate(false);
        cluster.node(cluster.leader().get().id()).crash();
        simulation.run(Duration.ofSeconds(5));
        assertThat(cluster.leaderCount()).isZero();

        follower.election().setCandidate(true);

        assertThat(simulation.runUntil(() -> follower.role() != Role.FOLLOWER, TIMINGS.electionTimeoutMin().dividedBy(2)))
                .as("its timer restarts when it stands again")
                .isFalse();
        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE)).isTrue();
    }

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }

    private static ElectionCluster.NodeUnderTest[] followers(ElectionCluster cluster) {
        return cluster.nodes().stream()
                .filter(node -> node.role() == Role.FOLLOWER && !node.isWitness())
                .toArray(ElectionCluster.NodeUnderTest[]::new);
    }
}
