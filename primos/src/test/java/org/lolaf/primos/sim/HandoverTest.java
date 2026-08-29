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
 * Graceful leadership handover on a clean stop — Raft thesis §3.10.
 *
 * <p>A leader that simply stops leaves the cluster without one until somebody's election timeout
 * expires, because a timeout is how a follower notices silence. Telling one follower to campaign
 * now collapses that wait to about a round trip. These tests are mostly about the difference
 * between those two numbers, which is the entire reason the message exists.
 */
class HandoverTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);
    private static final Timings TIMINGS = Timings.lan();

    @Test
    void a_leader_that_stops_cleanly_is_replaced_in_far_less_than_an_election_timeout() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int departing = cluster.leader().get().id();
        cluster.node(departing).stopCleanly();

        // Comfortably inside the minimum election timeout, so nothing here could have been the
        // ordinary timeout firing. That margin is the whole benefit.
        Duration wellInsideTheTimeout = TIMINGS.electionTimeoutMin().dividedBy(2);
        assertThat(simulation.runUntil(cluster::hasSingleLeader, wellInsideTheTimeout))
                .as("the departing leader named a successor instead of going quiet, so nobody had"
                        + " to wait out a timeout to discover it had gone")
                .isTrue();
        assertThat(cluster.leader().get().id()).isNotEqualTo(departing);
    }

    @Test
    void without_a_handover_the_cluster_waits_out_its_election_timeout() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        // The same departure, but abrupt: nothing is sent on the way out.
        cluster.node(cluster.leader().get().id()).crash();

        assertThat(simulation.runUntil(cluster::hasSingleLeader,
                TIMINGS.electionTimeoutMin().dividedBy(2)))
                .as("this is the wait the handover removes; if a crash were replaced this quickly"
                        + " the test above would be proving nothing")
                .isFalse();
        assertThat(simulation.runUntil(cluster::hasSingleLeader, SETTLE)).isTrue();
    }

    @Test
    void the_successor_takes_the_next_term_and_nobody_else_claims_it() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        long termBefore = cluster.leader().get().currentTerm();
        cluster.node(cluster.leader().get().id()).stopCleanly();
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        assertThat(cluster.leader().get().currentTerm())
                .as("skipping the pre-vote costs exactly one term, the one the successor campaigns"
                        + " in — not the runaway inflation pre-vote exists to prevent")
                .isEqualTo(termBefore + 1);
        assertThat(cluster.leaderCount()).isEqualTo(1);
    }

    @Test
    void a_witness_is_never_handed_leadership() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        cluster.node(cluster.leader().get().id()).stopCleanly();
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.node(ElectionCluster.WITNESS_ID).role())
                .as("declining to campaign is the whole of what makes a witness one, and an"
                        + " invitation to campaign must not be the exception")
                .isEqualTo(Role.FOLLOWER);
        assertThat(cluster.node(ElectionCluster.WITNESS_ID).election().handoversAccepted())
                .isZero();
    }

    @Test
    void a_witness_that_is_told_to_campaign_anyway_refuses() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest witness = cluster.node(ElectionCluster.WITNESS_ID);

        // Straight at the handler, because no correct leader would ever send this. The guard has
        // to hold against a sender that does not know the rule, not only against one that does.
        witness.election().onTimeoutNow(cluster.leader().get().id(), witness.currentTerm());
        simulation.run(Duration.ofSeconds(10));

        assertThat(witness.role()).isEqualTo(Role.FOLLOWER);
        assertThat(witness.election().handoversAccepted()).isZero();
    }

    @Test
    void a_handover_from_a_term_that_is_over_is_ignored() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        ElectionCluster.NodeUnderTest follower = cluster.nodes().stream()
                .filter(node -> node.role() == Role.FOLLOWER)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no follower"));
        long staleTerm = follower.currentTerm() - 1;

        follower.election().onTimeoutNow(cluster.leader().get().id(), staleTerm);

        assertThat(follower.role())
                .as("a delayed handover from a leader this node has already left behind would"
                        + " campaign on behalf of a term that is over")
                .isEqualTo(Role.FOLLOWER);
        assertThat(follower.election().handoversAccepted()).isZero();
    }

    @Test
    void a_follower_stopping_cleanly_hands_over_nothing() {
        Simulation simulation = simulation(7L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        int leader = cluster.leader().get().id();
        ElectionCluster.NodeUnderTest follower = cluster.nodes().stream()
                .filter(node -> node.role() == Role.FOLLOWER)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no follower"));
        long term = cluster.leader().get().currentTerm();

        follower.stopCleanly();
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.leader().get().id())
                .as("only a leader has anything to hand over; a follower leaving is not an event")
                .isEqualTo(leader);
        assertThat(cluster.leader().get().currentTerm()).isEqualTo(term);
    }

    private static Simulation simulation(long seed) {
        // Every scenario here runs under the full invariant set, so a handover that produced two
        // leaders in a term, or let a witness campaign, fails on the invariant rather than on an
        // assertion at the end.
        return new Simulation(seed).checking(Invariants.all());
    }

    // ------------------------------------------------- announcing a departure

    /**
     * The probe timeout a real JVM deployment would use.
     *
     * <p>Long on purpose, and not a strawman: a stop-the-world collection is indistinguishable
     * from a dead process, so operators raise this precisely so a garbage collection is not
     * reported as a failure. Every test below is about the gap that tuning opens — a node that
     * shut down politely would otherwise be reported alive for this entire window.
     */
    private static final Timings GC_TOLERANT = Timings.builder()
            .probeInterval(Duration.ofSeconds(10))
            .failureThreshold(3)
            .build();

    @Test
    void a_departing_follower_is_reported_down_at_once_rather_than_after_the_probe_timeout() {
        Simulation simulation = simulation(8L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false, GC_TOLERANT);
        settleWithReachability(simulation, cluster);

        ElectionCluster.NodeUnderTest leaving = follower(cluster);
        ElectionCluster.NodeUnderTest watcher = cluster.node(cluster.leader().get().id());
        assertThat(watcher.election().isReachable(leaving.id())).isTrue();

        leaving.stopCleanly();
        simulation.run(Duration.ofSeconds(1));

        assertThat(watcher.election().isReachable(leaving.id()))
                .as("silence would have taken 30 seconds to be believed here; the node knew it was"
                        + " going and said so")
                .isFalse();
        assertThat(watcher.election().departuresObserved()).isEqualTo(1);
    }

    @Test
    void without_the_announcement_the_same_departure_takes_the_whole_probe_timeout() {
        Simulation simulation = simulation(9L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false, GC_TOLERANT);
        settleWithReachability(simulation, cluster);

        ElectionCluster.NodeUnderTest leaving = follower(cluster);
        ElectionCluster.NodeUnderTest watcher = cluster.node(cluster.leader().get().id());

        // A crash says nothing, which is the only other way a node can go.
        leaving.crash();
        simulation.run(Duration.ofSeconds(1));

        assertThat(watcher.election().isReachable(leaving.id()))
                .as("this is the gap the announcement closes; if silence were noticed this fast"
                        + " the test above would be proving nothing")
                .isTrue();

        simulation.run(Duration.ofSeconds(40));
        assertThat(watcher.election().isReachable(leaving.id()))
                .as("and it is still detected eventually, by measurement rather than by being told")
                .isFalse();
    }

    @Test
    void a_departure_does_not_shrink_the_quorum() {
        Simulation simulation = simulation(10L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 5, false, GC_TOLERANT);
        settleWithReachability(simulation, cluster);

        // Three of five leave politely, so a witness to it has been told the cluster is down to
        // two. Quorum is configured, not observed: two is not a majority of five and must not
        // become one just because the others were well mannered about going.
        int leader = cluster.leader().get().id();
        int stopped = 0;
        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            if (node.id() != leader && stopped < 3) {
                node.stopCleanly();
                stopped++;
            }
        }
        simulation.run(Duration.ofSeconds(20));

        assertThat(cluster.leaderCount())
                .as("reading a departure as 'the cluster shrank' is how a split brain is talked"
                        + " into existence")
                .isZero();
    }

    @Test
    void a_departure_cannot_make_a_leader_stand_down() {
        Simulation simulation = simulation(11L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false, GC_TOLERANT);
        settleWithReachability(simulation, cluster);

        ElectionCluster.NodeUnderTest leader = cluster.node(cluster.leader().get().id());
        long term = leader.currentTerm();

        // Straight at the handler, twice, as a forgery or a replay would arrive. CheckQuorum reads
        // lastRoundTripNanos, which this must not touch — otherwise a captured datagram would be a
        // way to depose a healthy leader.
        for (ElectionCluster.NodeUnderTest peer : cluster.nodes()) {
            if (peer.id() != leader.id()) {
                leader.election().onLeaving(peer.id(), term);
            }
        }
        simulation.run(Duration.ofSeconds(1));

        assertThat(leader.role()).isEqualTo(Role.LEADER);
        assertThat(leader.currentTerm()).isEqualTo(term);
    }

    @Test
    void a_peer_that_comes_back_after_announcing_is_reported_up_again() {
        Simulation simulation = simulation(12L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false, GC_TOLERANT);
        settleWithReachability(simulation, cluster);

        ElectionCluster.NodeUnderTest leader = cluster.node(cluster.leader().get().id());
        ElectionCluster.NodeUnderTest other = follower(cluster);
        leader.election().onLeaving(other.id(), leader.currentTerm());

        assertThat(leader.election().isReachable(other.id())).isFalse();

        // It never actually went, so its next probe reply corrects the record. That self-healing
        // is what bounds what a forged departure can do to one probe interval.
        simulation.run(Duration.ofSeconds(25));

        assertThat(leader.election().isReachable(other.id()))
                .as("being told is a hint; measurement stays authoritative")
                .isTrue();
    }

    /**
     * Elects a leader and then waits for the probes to have actually established reachability.
     *
     * <p>Necessary because the probe interval here is deliberately long. A peer starts out
     * neither up nor down, and only becomes "up" when its first probe reply lands — so without
     * this the tests below would be asserting a transition away from a state nothing had reached.
     */
    private static void settleWithReachability(Simulation simulation, ElectionCluster cluster) {
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.runUntil(() -> cluster.nodes().stream().allMatch(node ->
                        cluster.nodes().stream()
                                .filter(peer -> peer.id() != node.id())
                                .allMatch(peer -> node.election().isReachable(peer.id()))),
                Duration.ofSeconds(60));
    }

    private static ElectionCluster.NodeUnderTest follower(ElectionCluster cluster) {
        return cluster.nodes().stream()
                .filter(node -> node.role() == Role.FOLLOWER && !node.isWitness())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no follower"));
    }
}
