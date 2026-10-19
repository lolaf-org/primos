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
package org.lolaf.primos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.transport.Ports;
import org.lolaf.primos.transport.UdpTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A real cluster: real sockets, real threads, real packets on the loopback interface.
 *
 * <p>A sanity check rather than the safety net. The deterministic simulation is what proves the
 * algorithm; this proves the algorithm is wired to a socket correctly, which is a different and
 * much shallower question — but one no amount of simulation can answer.
 */
class UdpClusterTest {

    private static final String LOOPBACK = "127.0.0.1";
    private static final byte[] SECRET = "a-shared-cluster-secret".getBytes();

    private static final Timings FAST = Timings.builder()
            .heartbeatInterval(Duration.ofMillis(25))
            .electionTimeoutMin(Duration.ofMillis(150))
            .electionTimeoutMax(Duration.ofMillis(300))
            .probeInterval(Duration.ofMillis(100))
            .build();

    private final List<PrimosNode> nodes = new ArrayList<>();
    private final RecordingStats drops = new RecordingStats();

    private static int leaders(PrimosNode... nodes) {
        int count = 0;
        for (PrimosNode node : nodes) {
            if (node.role() == Role.LEADER) {
                count++;
            }
        }
        return count;
    }

    private static PrimosNode leaderOf(PrimosNode... nodes) {
        for (PrimosNode node : nodes) {
            if (node.role() == Role.LEADER) {
                return node;
            }
        }
        throw new IllegalStateException("no leader");
    }

    /**
     * The id of the node this one believes is leading, or -1 while none is known — the shape an
     * awaited condition wants, without an Optional per poll.
     */
    private static int leaderId(PrimosNode node) {
        return node.leader().map(Leader::id).orElse(-1);
    }

    @AfterEach
    void stopEverything() {
        nodes.forEach(PrimosNode::stop);
    }

    @Test
    void three_nodes_on_real_sockets_elect_exactly_one_leader() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        assertThat(leaders(one, two, three)).isEqualTo(1);
        assertThat(drops.totalDrops())
                .as("a healthy cluster on a quiet loopback drops nothing")
                .isZero();
    }

    @Test
    void every_node_agrees_who_leads() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() ->
                leaderId(one) > 0 && leaderId(one) == leaderId(two)
                        && leaderId(two) == leaderId(three));

        int leaderId = leaderId(one);
        assertThat(leaderId(three)).isEqualTo(leaderId);

        PrimosNode follower = one.leader().orElseThrow(AssertionError::new).isSelf() ? two : one;
        assertThat(follower.leader().flatMap(Leader::peer)).map(Peer::port)
                .as("a follower is handed the address to forward work to, not an id to look up")
                .hasValue(ports[leaderId - 1]);
    }

    @Test
    void the_survivors_replace_a_leader_that_is_stopped() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);
        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        PrimosNode leader = leaderOf(one, two, three);
        leader.stop();

        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);
        assertThat(leaderOf(one, two, three)).isNotSameAs(leader);
    }

    @Test
    void peers_become_reachable_over_a_real_network() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        start(2, ports, null, new MemoryStateStore(), ports[1]);
        start(3, ports, null, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() -> drops.probeRoundTrips.size() >= 2);

        assertThat(drops.probeRoundTrips)
                .as("real round trips on loopback, measured — every sample rather than the last")
                .isNotEmpty()
                .allMatch(nanos -> nanos > 0, "a positive round trip");
    }

    @Test
    void a_published_change_reaches_every_follower_through_its_listener() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        List<String> told = java.util.Collections.synchronizedList(new ArrayList<>());
        PrimosListener recorder = new PrimosListener() {
            @Override
            public void onLeaderVersion(Leader leader, long term, long version) {
                told.add(term + "/" + version + (leader.isSelf() ? "/self" : ""));
            }
        };
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);
        for (PrimosNode node : new PrimosNode[] {one, two, three}) {
            node.addListener(recorder);
        }
        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);
        PrimosNode leader = leaderOf(one, two, three);
        PrimosNode follower = leader == one ? two : one;

        assertThat(follower.publishChange()).as("a follower has nothing to publish").isEmpty();
        PublishedVersion published = leader.publishChange().orElseThrow(AssertionError::new);

        String expected = published.term() + "/" + published.version();
        await().atMost(5, TimeUnit.SECONDS).until(() -> told.size() == 3);
        assertThat(told).containsExactlyInAnyOrder(expected + "/self", expected, expected);
    }

    /** Records which peers a node reported down, since that is no longer readable from a snapshot. */
    private static final class RecordingListener implements PrimosListener {

        private final java.util.Set<Integer> down =
                java.util.Collections.synchronizedSet(new java.util.HashSet<>());

        @Override
        public void onPeerDown(Peer peer, PeerDownReason reason) {
            down.add(peer.id());
        }

        @Override
        public void onPeerUp(Peer peer) {
            down.remove(peer.id());
        }
    }

    // -------------------------------------------------------------- helpers

    @Test
    void a_cluster_forms_with_authentication_enabled() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, SECRET, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, SECRET, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, SECRET, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        assertThat(drops.totalDrops())
                .as("an authenticated cluster on loopback drops nothing, for any reason")
                .isZero();
    }

    @Test
    void nodes_with_different_secrets_never_form_a_cluster() throws Exception {
        int[] ports = {Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, SECRET, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, "a-different-secret".getBytes(),
                new MemoryStateStore(), ports[1]);

        Thread.sleep(3_000);

        assertThat(leaders(one, two))
                .as("neither can authenticate the other, so neither can ever reach a quorum of two")
                .isZero();
        assertThat(drops.drops(org.lolaf.primos.DropReason.BAD_TAG))
                .as("and the reason is diagnosable from a counter, not a packet capture")
                .isPositive();
    }

    @Test
    void a_node_that_returns_on_a_different_port_is_found_again() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        StateStore thirdStore = new MemoryStateStore();
        PrimosNode one = start(1, ports, SECRET, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, SECRET, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, SECRET, thirdStore, ports[2]);
        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        // Attached before the stop: a departure is announced at once, so a listener registered
        // afterwards would have missed the very event it is waiting for.
        RecordingListener onOne = new RecordingListener();
        one.addListener(onOne);
        three.stop();
        await().atMost(15, TimeUnit.SECONDS).until(() -> onOne.down.contains(3));

        // Same identity, new endpoint: exactly what a container restart looks like.
        PrimosNode moved = start(3, ports, SECRET, thirdStore, Ports.free());

        await().atMost(15, TimeUnit.SECONDS).until(() -> !onOne.down.contains(3));
        assertThat(moved.role()).isNotEqualTo(Role.FAILED);
    }

    private PrimosNode start(int id, int[] ports, byte[] secret, StateStore store, int bindPort) {
        List<Peer> peers = new ArrayList<>();
        for (int peerId = 1; peerId <= ports.length; peerId++) {
            if (peerId != id) {
                peers.add(Peer.application(peerId, LOOPBACK, ports[peerId - 1]));
            }
        }
        try {
            UdpTransport transport = UdpTransport.builder()
                    .bindAddress(new InetSocketAddress(LOOPBACK, bindPort))
                    .peers(peers)
                    .addressLearning(secret != null)
                    .clusterName("udp-test")
                    .build();

            PrimosNode node = PrimosNode.builder()
                    .statsListener(drops)
                    .clusterConfig(ClusterConfig.builder()
                            .applicationName("udp-test")
                            .nodeId(id)
                            .peers(peers)
                            .timings(FAST)
                            .build())
                    .store(() -> store)
                    .transport(() -> transport)
                    .clusterSecret(secret)
                    .build();
            node.start();
            nodes.add(node);
            return node;
        } catch (IOException e) {
            throw new IllegalStateException("cannot start node " + id, e);
        }
    }

    @Test
    void a_leader_stopped_cleanly_hands_over_rather_than_going_quiet() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);
        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        PrimosNode leader = leaderOf(one, two, three);
        // The real shutdown path, not the simulation's: stop() runs on this thread while the
        // handover has to go out from the node's own, which is the wiring under test here.
        leader.stop();

        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        PrimosNode successor = leaderOf(one, two, three);
        assertThat(successor).isNotSameAs(leader);
        assertThat(drops.handoversAccepted.get())
                .as("the successor campaigned because it was told to, not because it timed out"
                        + " waiting — the timeout would have replaced the leader too, just later")
                .isEqualTo(1);
    }

    @Test
    void a_follower_stopped_cleanly_is_reported_down_without_waiting_for_probes() {
        int[] ports = {Ports.free(), Ports.free(), Ports.free()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);
        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        // A follower, so nothing about leadership is in play — only the reporting.
        PrimosNode follower = one.role() == Role.LEADER ? two : one;
        PrimosNode watcher = follower == one ? two : one;
        int departing = follower.config().nodeId();
        RecordingListener onWatcher = new RecordingListener();
        watcher.addListener(onWatcher);
        follower.stop();

        // Comfortably inside the probe timeout, which is what the announcement buys. On a real
        // deployment that timeout is tuned up for garbage collection, making this gap much larger.
        await().atMost(2, TimeUnit.SECONDS).until(() -> onWatcher.down.contains(departing));

        assertThat(onWatcher.down).contains(departing);
    }
}
