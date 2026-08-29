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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.transport.UdpTransport;

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
            .electionTimeout(Duration.ofMillis(150), Duration.ofMillis(300))
            .probeInterval(Duration.ofMillis(100))
            .build();

    private final List<PrimosNode> nodes = new ArrayList<>();

    @AfterEach
    void stopEverything() {
        nodes.forEach(PrimosNode::stop);
    }

    @Test
    void three_nodes_on_real_sockets_elect_exactly_one_leader() {
        int[] ports = {freePort(), freePort(), freePort()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        assertThat(leaders(one, two, three)).isEqualTo(1);
        assertThat(one.stats().totalDrops())
                .as("a healthy cluster on a quiet loopback drops nothing")
                .isZero();
    }

    @Test
    void every_node_agrees_who_leads() {
        int[] ports = {freePort(), freePort(), freePort()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() ->
                one.leaderId().isPresent() && two.leaderId().isPresent()
                        && three.leaderId().isPresent()
                        && one.leaderId().equals(two.leaderId())
                        && two.leaderId().equals(three.leaderId()));

        assertThat(one.leaderId()).isEqualTo(three.leaderId());
    }

    @Test
    void the_survivors_replace_a_leader_that_is_stopped() {
        int[] ports = {freePort(), freePort(), freePort()};
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
        int[] ports = {freePort(), freePort(), freePort()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        start(2, ports, null, new MemoryStateStore(), ports[1]);
        start(3, ports, null, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() ->
                one.stats().peers().values().stream().allMatch(peer -> peer.reachable()));

        assertThat(one.stats().peers().get(2).lastRoundTrip())
                .as("a real round trip on loopback, measured")
                .isNotNull();
    }

    @Test
    void a_cluster_forms_with_authentication_enabled() {
        int[] ports = {freePort(), freePort(), freePort()};
        PrimosNode one = start(1, ports, SECRET, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, SECRET, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, SECRET, new MemoryStateStore(), ports[2]);

        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        assertThat(one.stats().drops()).allSatisfy((reason, count) ->
                assertThat(count).as("reason %s", reason).isZero());
    }

    @Test
    void nodes_with_different_secrets_never_form_a_cluster() throws Exception {
        int[] ports = {freePort(), freePort()};
        PrimosNode one = start(1, ports, SECRET, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, "a-different-secret".getBytes(),
                new MemoryStateStore(), ports[1]);

        Thread.sleep(3_000);

        assertThat(leaders(one, two))
                .as("neither can authenticate the other, so neither can ever reach a quorum of two")
                .isZero();
        assertThat(one.stats().drops().get(org.lolaf.primos.DropReason.BAD_TAG))
                .as("and the reason is diagnosable from a counter, not a packet capture")
                .isPositive();
    }

    @Test
    void a_node_that_returns_on_a_different_port_is_found_again() {
        int[] ports = {freePort(), freePort(), freePort()};
        StateStore thirdStore = new MemoryStateStore();
        PrimosNode one = start(1, ports, SECRET, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, SECRET, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, SECRET, thirdStore, ports[2]);
        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);

        three.stop();
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> !one.stats().peers().get(3).reachable());

        // Same identity, new endpoint: exactly what a container restart looks like.
        PrimosNode moved = start(3, ports, SECRET, thirdStore, freePort());

        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> one.stats().peers().get(3).reachable());
        assertThat(moved.role()).isNotEqualTo(Role.FAILED);
    }

    // -------------------------------------------------------------- helpers

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
                    .clusterConfig(ClusterConfig.builder()
                            .applicationName("udp-test")
                            .nodeId(id)
                            .peers(peers)
                            .timings(FAST)
                            .build())
                    .store(store)
                    .transport(transport)
                    .clusterSecret(secret)
                    .build();
            node.start();
            nodes.add(node);
            return node;
        } catch (IOException e) {
            throw new IllegalStateException("cannot start node " + id, e);
        }
    }

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

    private static int freePort() {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("no free port", e);
        }
    }

    @Test
    void a_leader_stopped_cleanly_hands_over_rather_than_going_quiet() {
        int[] ports = {freePort(), freePort(), freePort()};
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
        assertThat(successor.stats().handoversAccepted())
                .as("the successor campaigned because it was told to, not because it timed out"
                        + " waiting — the timeout would have replaced the leader too, just later")
                .isEqualTo(1);
    }

    @Test
    void a_follower_stopped_cleanly_is_reported_down_without_waiting_for_probes() {
        int[] ports = {freePort(), freePort(), freePort()};
        PrimosNode one = start(1, ports, null, new MemoryStateStore(), ports[0]);
        PrimosNode two = start(2, ports, null, new MemoryStateStore(), ports[1]);
        PrimosNode three = start(3, ports, null, new MemoryStateStore(), ports[2]);
        await().atMost(15, TimeUnit.SECONDS).until(() -> leaders(one, two, three) == 1);
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                one.stats().peers().values().stream().allMatch(peer -> peer.reachable()));

        // A follower, so nothing about leadership is in play — only the reporting.
        PrimosNode follower = one.role() == Role.LEADER ? two : one;
        PrimosNode watcher = follower == one ? two : one;
        int departing = follower.config().nodeId();
        follower.stop();

        // Comfortably inside the probe timeout, which is what the announcement buys. On a real
        // deployment that timeout is tuned up for garbage collection, making this gap much larger.
        await().atMost(2, TimeUnit.SECONDS)
                .until(() -> !watcher.stats().peers().get(departing).reachable());

        assertThat(watcher.stats().peers().get(departing).reachable()).isFalse();
    }
}
