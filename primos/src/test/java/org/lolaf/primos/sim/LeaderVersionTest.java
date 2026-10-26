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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Leader;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.PublishedVersion;
import org.lolaf.primos.core.ElectionNode;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.SimulatedNetwork;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.WireFormat;

class LeaderVersionTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);
    private static final Duration A_FEW_HEARTBEATS = Duration.ofMillis(200);

    private final Map<Integer, List<String>> reported = new HashMap<>();

    // ------------------------------------------------------------------ a cluster

    @Test
    void every_node_is_told_once_per_publication_and_the_leader_sees_itself() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 3, false));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        long term = leader.currentTerm();

        PublishedVersion published = leader.election().publishChange().get();
        simulation.run(A_FEW_HEARTBEATS);

        assertThat(published).isEqualTo(new PublishedVersion(term, 1));
        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            String expected = term + "/1" + (node == leader ? "/self" : "/" + leader.id());
            assertThat(reported(node.id())).as("node %s", node.id()).containsExactly(expected);
        }
    }

    @Test
    void heartbeats_repeating_a_version_report_nothing_more() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 3, false));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        cluster.leader().get().election().publishChange();

        simulation.run(Duration.ofSeconds(10));

        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            assertThat(reported(node.id()))
                    .as("node %s heard about two hundred heartbeats carrying version 1", node.id())
                    .hasSize(1);
        }
    }

    @Test
    void a_leader_that_has_published_nothing_is_not_reported() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 3, false));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        simulation.run(Duration.ofSeconds(2));

        assertThat(reported).allSatisfy((id, events) -> assertThat(events).isEmpty());
    }

    @Test
    void a_follower_that_missed_the_heartbeats_is_told_by_the_first_one_it_hears() {
        Simulation simulation = simulation(4L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 3, false));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        int cut = otherThan(cluster, leader.id());
        int heard = otherThan(cluster, leader.id(), cut);

        simulation.network().sever(leader.id(), cut);
        leader.election().publishChange();
        simulation.run(Duration.ofMillis(150));

        assertThat(reported(heard)).as("the follower still in contact").hasSize(1);
        assertThat(reported(cut)).as("the follower cut off, while it is").isEmpty();

        simulation.network().restore(leader.id(), cut);
        simulation.run(A_FEW_HEARTBEATS);

        assertThat(reported(cut)).containsExactly(leader.currentTerm() + "/1/" + leader.id());
    }

    @Test
    void a_new_leader_starts_again_at_one_and_is_reported_although_the_version_is_lower() {
        Simulation simulation = simulation(5L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 3, false));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest first = cluster.leader().get();
        long firstTerm = first.currentTerm();
        first.election().publishChange();
        first.election().publishChange();
        simulation.run(A_FEW_HEARTBEATS);

        first.crash();
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest second = cluster.leader().get();
        int follower = otherThan(cluster, first.id(), second.id());
        simulation.run(A_FEW_HEARTBEATS);

        assertThat(reported(follower)).as("nothing published yet in the new term")
                .containsExactly(firstTerm + "/2/" + first.id());

        assertThat(second.election().publishChange().get().version()).isEqualTo(1);
        simulation.run(A_FEW_HEARTBEATS);

        assertThat(reported(follower)).last().isEqualTo(second.currentTerm() + "/1/" + second.id());
    }

    @Test
    void publications_between_two_heartbeats_are_reported_as_the_latest_one() {
        Simulation simulation = simulation(8L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 3, false));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest leader = cluster.leader().get();
        int follower = otherThan(cluster, leader.id());

        leader.election().publishChange();
        leader.election().publishChange();
        leader.election().publishChange();
        simulation.run(A_FEW_HEARTBEATS);

        assertThat(reported(follower))
                .as("a follower fetches the latest data, so versions 1 and 2 are never worth a call")
                .containsExactly(leader.currentTerm() + "/3/" + leader.id());
    }

    @Test
    void a_node_that_is_not_leading_publishes_nothing() {
        Simulation simulation = simulation(6L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 3, false));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest follower = cluster.node(otherThan(cluster, cluster.leader().get().id()));

        assertThat(follower.election().publishChange()).isEmpty();
    }

    @Test
    void a_witness_is_never_told() {
        Simulation simulation = simulation(7L);
        ElectionCluster cluster = recorded(ElectionCluster.of(simulation, 2, true));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        cluster.leader().get().election().publishChange();

        simulation.run(A_FEW_HEARTBEATS);

        assertThat(reported(ElectionCluster.WITNESS_ID)).isEmpty();
        assertThat(reported(otherThan(cluster, cluster.leader().get().id(), ElectionCluster.WITNESS_ID)))
                .hasSize(1);
    }

    // ------------------------------------------------- heartbeats out of order

    // A lone follower and hand-made heartbeats from node 2, delivered as datagrams: a duplicated or
    // reordered heartbeat is exactly what no correct leader sends on purpose and UDP delivers anyway.

    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network = new SimulatedNetwork(1L, clock);
    private final MessageEncoder leaderEncoder = new MessageEncoder("t", 2, MessageAuthenticator.disabled());

    @Test
    void a_duplicated_or_reordered_heartbeat_reports_nothing() {
        ElectionNode follower = loneFollower();

        heartbeat(follower, 1, 2);
        heartbeat(follower, 1, 2);
        heartbeat(follower, 1, 1);
        heartbeat(follower, 1, 3);

        assertThat(reported(1)).containsExactly("1/2/2", "1/3/2");
    }

    @Test
    void a_higher_term_is_reported_whatever_its_version() {
        ElectionNode follower = loneFollower();

        heartbeat(follower, 1, 5);
        heartbeat(follower, 2, 1);
        heartbeat(follower, 1, 9);

        assertThat(reported(1))
                .as("the term-1 heartbeat arriving last is stale, and Raft's term rule refuses it")
                .containsExactly("1/5/2", "2/1/2");
    }

    @Test
    void version_zero_is_never_reported() {
        ElectionNode follower = loneFollower();

        heartbeat(follower, 1, 0);
        heartbeat(follower, 2, 0);

        assertThat(reported(1)).isEmpty();
        assertThat(follower.leader()).as("the leader is still known, through onLeaderChanged").isNotNull();
    }

    // ------------------------------------------------------------------ helpers

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }

    private ElectionCluster recorded(ElectionCluster cluster) {
        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            node.attach(recorder(node.id()));
        }
        return cluster;
    }

    private PrimosListener recorder(int id) {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        reported.put(id, events);
        return new PrimosListener() {
            @Override
            public void onLeaderVersion(Leader leader, long term, long version) {
                events.add(term + "/" + version + "/" + (leader.isSelf() ? "self" : leader.id()));
            }
        };
    }

    private List<String> reported(int id) {
        return reported.getOrDefault(id, Collections.emptyList());
    }

    private static int otherThan(ElectionCluster cluster, int... excluded) {
        return cluster.nodes().stream()
                .mapToInt(ElectionCluster.NodeUnderTest::id)
                .filter(id -> java.util.Arrays.stream(excluded).noneMatch(x -> x == id))
                .findFirst()
                .getAsInt();
    }

    private ElectionNode loneFollower() {
        List<Peer> peers = new ArrayList<>();
        peers.add(Peer.application(2, "sim", 7002));
        peers.add(Peer.application(3, "sim", 7003));
        ClusterConfig config = ClusterConfig.builder().applicationName("t").nodeId(1).peers(peers).build();
        ElectionNode node = new ElectionNode(config, new MemoryStateStore(), network.transportFor(1), clock,
                new Random(1L), MessageAuthenticator.disabled(), recorder(1));
        node.start();
        return node;
    }

    private void heartbeat(ElectionNode follower, long term, long version) {
        ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
        network.transportFor(2).send(1, leaderEncoder.appendEntries(buffer, term, version, 0));
        clock.advanceTo(Math.max(clock.nanoTime(), network.nextDeliveryNanos(1)));
        follower.runOnce(0L);
    }
}
