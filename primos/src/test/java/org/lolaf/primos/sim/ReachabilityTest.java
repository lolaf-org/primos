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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PeerType;
import org.lolaf.primos.RecordingStats;

/**
 * Who each node can actually reach, and what it tells the application about it.
 *
 * <p>Every node probes every peer rather than relaying the leader's opinion, so these answers stay
 * meaningful during an election, during a partition, and when there is no leader at all — which is
 * precisely when somebody is asking.
 */
class ReachabilityTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);

    @Test
    void every_peer_is_reported_up_once_it_answers() {
        Simulation simulation = new Simulation(1L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        RecordingListener events = listenOn(cluster, 1);

        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.node(1).election().isReachable(2)).isTrue();
        assertThat(cluster.node(1).election().isReachable(3)).isTrue();
        assertThat(events.up).containsExactlyInAnyOrder(2, 3);
        assertThat(events.down).isEmpty();
    }

    @Test
    void a_peer_that_stops_answering_is_reported_down() {
        Simulation simulation = new Simulation(2L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        RecordingListener events = listenOn(cluster, 1);
        simulation.run(Duration.ofSeconds(5));

        simulation.crash(3);
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.node(1).election().isReachable(3)).isFalse();
        assertThat(cluster.node(1).election().isReachable(2)).isTrue();
        assertThat(events.down).containsExactly(3);
    }

    @Test
    void it_is_reported_up_again_when_it_comes_back() {
        Simulation simulation = new Simulation(3L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        RecordingListener events = listenOn(cluster, 1);
        simulation.run(Duration.ofSeconds(5));

        simulation.crash(3);
        simulation.run(Duration.ofSeconds(10));
        simulation.restart(3);
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.node(1).election().isReachable(3)).isTrue();
        assertThat(events.down).containsExactly(3);
        assertThat(events.up).containsExactly(2, 3, 3);
    }

    @Test
    void an_event_fires_once_per_transition_not_once_per_probe() {
        Simulation simulation = new Simulation(4L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        RecordingListener events = listenOn(cluster, 1);
        simulation.run(Duration.ofSeconds(5));

        simulation.crash(3);
        simulation.run(Duration.ofSeconds(60));

        assertThat(events.down)
                .as("sixty seconds of failed probes is still one peer going down")
                .containsExactly(3);
    }

    @Test
    void a_partition_is_reported_from_both_sides() {
        Simulation simulation = new Simulation(5L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.run(Duration.ofSeconds(5));

        simulation.network().partition(new int[] {1}, new int[] {2, 3});
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.node(1).election().isReachable(2)).isFalse();
        assertThat(cluster.node(2).election().isReachable(1)).isFalse();
        assertThat(cluster.node(2).election().isReachable(3))
                .as("the other side of the split still sees itself perfectly well")
                .isTrue();
    }

    @Test
    void a_one_way_cut_is_noticed_by_the_side_whose_probes_go_unanswered() {
        Simulation simulation = new Simulation(6L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.run(Duration.ofSeconds(5));

        simulation.network().sever(2, 1);
        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.node(2).election().isReachable(1))
                .as("its probes arrive but no reply comes back, so it has no round trip")
                .isFalse();
    }

    @Test
    void a_delayed_reply_to_an_abandoned_probe_does_not_revive_a_dead_peer() {
        Simulation simulation = new Simulation(7L).checking(Invariants.all());
        simulation.network().delayRange(0, java.util.concurrent.TimeUnit.SECONDS.toNanos(20));
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);

        simulation.run(Duration.ofSeconds(120));

        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            assertThat(node.election().trackedPeerIds())
                    .as("the peer set survives twenty-second delays; what must not happen is a"
                            + " reply to an abandoned probe reviving a peer, which the invariants"
                            + " check on every step")
                    .isNotEmpty();
        }
    }

    @Test
    void the_witness_is_probed_like_any_other_peer_and_is_distinguishable() {
        Simulation simulation = new Simulation(8L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 2, true);
        RecordingListener events = listenOn(cluster, 1);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.run(Duration.ofSeconds(5));

        assertThat(cluster.node(1).election().isReachable(ElectionCluster.WITNESS_ID)).isTrue();
        assertThat(events.witnessesUp)
                .as("witness down is a different alert from peer down: nothing looks broken, but"
                        + " the cluster can no longer survive a partition")
                .containsExactly(ElectionCluster.WITNESS_ID);
    }

    @Test
    void a_round_trip_time_is_recorded_for_each_peer() {
        Simulation simulation = new Simulation(9L).checking(Invariants.all());
        simulation.network().delayRange(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(20),
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(20));
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        RecordingStats recorded = new RecordingStats();
        cluster.node(1).attach(recorded);

        simulation.run(Duration.ofSeconds(10));

        assertThat(cluster.node(1).election().isReachable(2)).isTrue();
        assertThat(recorded.probeRoundTrips)
                .as("every sample, which is what a percentile needs and what a last-value gauge"
                        + " could never have given")
                .isNotEmpty()
                .allMatch(nanos -> nanos == Duration.ofMillis(40).toNanos(),
                        "twenty milliseconds each way");
    }

    @Test
    void reachability_still_answers_while_there_is_no_leader_at_all() {
        Simulation simulation = new Simulation(10L).checking(Invariants.all());
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.run(Duration.ofSeconds(5));

        simulation.crash(2);
        simulation.crash(3);
        simulation.run(Duration.ofSeconds(15));

        assertThat(cluster.leaderCount()).isZero();
        assertThat(cluster.node(1).election().isReachable(2))
                .as("no leader exists to relay an opinion, and the answer is still correct")
                .isFalse();
        assertThat(cluster.node(1).election().isReachable(3)).isFalse();
    }

    private static RecordingListener listenOn(ElectionCluster cluster, int nodeId) {
        RecordingListener listener = new RecordingListener();
        cluster.node(nodeId).attach(listener);
        return listener;
    }

    /** Records the peer events a node emits. */
    static final class RecordingListener implements org.lolaf.primos.PrimosListener {

        final List<Integer> up = new ArrayList<>();
        final List<Integer> down = new ArrayList<>();
        final List<Integer> witnessesUp = new ArrayList<>();

        @Override
        public void onPeerUp(Peer peer) {
            up.add(peer.id());
            if (peer.isWitness()) {
                witnessesUp.add(peer.id());
            }
        }

        @Override
        public void onPeerDown(Peer peer) {
            down.add(peer.id());
        }
    }
}
