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
package org.lolaf.primos.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.primos.DropReason;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.Role;
import org.lolaf.primos.Timings;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.transport.LoopbackNetwork;

/** The public API, running for real: its own thread, its own socket, its own clock. */
class PrimosNodeTest {

    private static final Timings FAST = Timings.builder()
            .heartbeatInterval(Duration.ofMillis(20))
            .electionTimeout(Duration.ofMillis(100), Duration.ofMillis(200))
            .probeInterval(Duration.ofMillis(100))
            .build();

    private final LoopbackNetwork network = new LoopbackNetwork();
    private final List<PrimosNode> started = new ArrayList<>();
    private ExecutorService executor;

    @AfterEach
    void stopEverything() {
        started.forEach(PrimosNode::stop);
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    // ------------------------------------------------------------- lifecycle

    @Test
    void a_single_node_leads_itself() {
        PrimosNode node = start(builder(1));

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);
        assertThat(node.roleAndTerm().isLeader()).isTrue();
        assertThat(node.leaderId()).hasValue(1);
    }

    @Test
    void start_returns_before_a_leader_exists_rather_than_pretending_otherwise() {
        PrimosNode node = start(builder(1, 2, 3));

        assertThat(node.role())
                .as("an election takes at least one election timeout; start() must not block for it")
                .isEqualTo(Role.FOLLOWER);
    }

    @Test
    void starting_twice_is_refused() {
        PrimosNode node = start(builder(1));

        assertThatThrownBy(node::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @Test
    void stopping_twice_is_harmless_because_it_ends_up_in_shutdown_hooks() {
        PrimosNode node = start(builder(1));

        assertThatCode(() -> {
            node.stop();
            node.stop();
        }).doesNotThrowAnyException();
    }

    @Test
    void a_stopped_node_is_terminal() {
        PrimosNode node = start(builder(1));
        node.stop();

        assertThatThrownBy(node::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stopped node is terminal");
    }

    @Test
    void a_stopped_node_stops_claiming_leadership() {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);

        node.stop();

        assertThat(node.role())
                .as("an application that checks after shutdown must not be told it is in charge")
                .isNotEqualTo(Role.LEADER);
        assertThat(node.roleAndTerm().isLeader()).isFalse();
        assertThat(node.leaderId()).isEmpty();
    }

    @Test
    void stopping_tells_the_listeners_that_leadership_is_over() {
        Recording events = new Recording();
        PrimosNode node = start(builder(1).listener(events));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);

        node.stop();

        assertThat(events.roles).endsWith("LEADER->FOLLOWER");
        assertThat(events.leaders).endsWith("none");
    }

    @Test
    void it_can_be_closed_in_try_with_resources() {
        PrimosNode node = builder(1).build();
        node.start();

        assertThatCode(node::close).doesNotThrowAnyException();
    }

    @Test
    void the_node_runs_one_daemon_thread_named_after_the_cluster() {
        start(builder(1));

        List<Thread> ours = Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("primos-orders-1"))
                .collect(java.util.stream.Collectors.toList());

        assertThat(ours).hasSize(1);
        assertThat(ours.get(0).isDaemon())
                .as("a forgotten node must never hold a JVM open")
                .isTrue();
    }

    // ------------------------------------------------------------- a cluster

    @Test
    void three_real_nodes_on_three_real_threads_elect_one_leader() {
        PrimosNode one = start(builder(1, 2, 3));
        PrimosNode two = start(builder(2, 1, 3));
        PrimosNode three = start(builder(3, 1, 2));

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> leaderCount(one, two, three) == 1);

        assertThat(leaderCount(one, two, three)).isEqualTo(1);
    }

    @Test
    void the_followers_learn_who_leads() {
        PrimosNode one = start(builder(1, 2, 3));
        PrimosNode two = start(builder(2, 1, 3));
        PrimosNode three = start(builder(3, 1, 2));

        await().atMost(10, TimeUnit.SECONDS).until(() ->
                one.leaderId().isPresent() && two.leaderId().isPresent()
                        && three.leaderId().isPresent());

        OptionalInt leader = one.leaderId();
        assertThat(two.leaderId()).isEqualTo(leader);
        assertThat(three.leaderId()).isEqualTo(leader);
    }

    @Test
    void role_is_answerable_from_any_thread() throws Exception {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);

        executor = Executors.newSingleThreadExecutor();
        Role fromElsewhere = executor.submit(node::role).get(5, TimeUnit.SECONDS);

        assertThat(fromElsewhere).isEqualTo(Role.LEADER);
    }

    // ------------------------------------------------------------- listeners

    @Test
    void a_listener_registered_before_start_sees_the_first_transition() {
        Recording events = new Recording();
        PrimosNode node = start(builder(1).listener(events));

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);
        await().atMost(5, TimeUnit.SECONDS).until(() -> events.roles.size() >= 2);

        assertThat(events.roles)
                .as("every transition is reported, not just the final state")
                .containsExactly("FOLLOWER->CANDIDATE", "CANDIDATE->LEADER");
        assertThat(events.leaders).contains("1");
    }

    @Test
    void a_listener_added_after_start_is_told_the_state_it_missed() {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);

        Recording late = new Recording();
        node.addListener(late);

        assertThat(late.roles)
                .as("it registered too late for the transition, so it is told where things stand")
                .containsExactly("LEADER->LEADER");
        assertThat(late.leaders).containsExactly("1");
    }

    @Test
    void a_listener_that_throws_neither_kills_the_node_nor_silences_the_others() {
        Recording good = new Recording();
        PrimosNode node = start(builder(1)
                .listener(new PrimosListener() {
                    @Override
                    public void onRoleChanged(Role previous, Role current, long term) {
                        throw new IllegalStateException("this listener is broken");
                    }
                })
                .listener(good));

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);
        await().atMost(5, TimeUnit.SECONDS).until(() -> good.roles.size() >= 2);

        assertThat(good.roles).containsExactly("FOLLOWER->CANDIDATE", "CANDIDATE->LEADER");
        assertThat(node.role()).isEqualTo(Role.LEADER);
    }

    @Test
    void callbacks_run_on_the_node_thread_by_default() {
        Recording events = new Recording();
        start(builder(1).listener(events));

        await().atMost(5, TimeUnit.SECONDS).until(() -> !events.threads.isEmpty());

        assertThat(events.threads).allSatisfy(name ->
                assertThat(name).startsWith("primos-orders-1"));
    }

    @Test
    void an_executor_moves_dispatch_off_the_node_thread() {
        executor = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "my-listener-thread"));
        Recording events = new Recording();
        start(builder(1).listener(events).listenerExecutor(executor));

        await().atMost(5, TimeUnit.SECONDS).until(() -> !events.threads.isEmpty());

        assertThat(events.threads).allSatisfy(name ->
                assertThat(name).isEqualTo("my-listener-thread"));
    }

    @Test
    void a_slow_listener_does_not_break_the_node() {
        PrimosNode node = start(builder(1).listener(new PrimosListener() {
            @Override
            public void onRoleChanged(Role previous, Role current, long term) {
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }));

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);
        assertThat(node.role()).isEqualTo(Role.LEADER);
    }

    // ---------------------------------------------------------------- stats

    @Test
    void stats_report_what_the_node_has_been_doing() {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.LEADER);
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.stats().electionsWon() > 0);

        assertThat(node.stats().role()).isEqualTo(Role.LEADER);
        assertThat(node.stats().currentTerm()).isPositive();
        assertThat(node.stats().electionsStarted()).isPositive();
        assertThat(node.stats().timeInRole()).isGreaterThanOrEqualTo(Duration.ZERO);
        assertThat(node.stats().drops()).containsKey(DropReason.BAD_TAG);
        assertThat(node.stats().totalDrops()).isZero();
    }

    @Test
    void stats_include_every_peer_once_a_cluster_is_running() {
        PrimosNode one = start(builder(1, 2, 3));
        start(builder(2, 1, 3));
        start(builder(3, 1, 2));

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> one.stats().peers().values().stream().allMatch(p -> p.reachable()));

        assertThat(one.stats().peers()).containsOnlyKeys(2, 3);
    }

    // -------------------------------------------------------------- builder

    @Test
    void a_node_without_a_store_is_refused_with_the_reason() {
        assertThatThrownBy(() -> PrimosNode.builder()
                .applicationName("orders").nodeId(1)
                .transport(network.transportFor(1))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("state store is required")
                .hasMessageContaining("elect two leaders");
    }

    @Test
    void a_node_without_a_transport_is_refused() {
        assertThatThrownBy(() -> PrimosNode.builder()
                .applicationName("orders").nodeId(1)
                .store(new MemoryStateStore())
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transport is required");
    }

    @Test
    void the_builder_applies_the_configuration_rules() {
        assertThatThrownBy(() -> PrimosNode.builder()
                .applicationName("a name with spaces").nodeId(1)
                .store(new MemoryStateStore())
                .transport(network.transportFor(1))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("printable ASCII");
    }

    @Test
    void a_secret_is_copied_so_the_caller_can_wipe_theirs() {
        byte[] secret = "a-shared-cluster-secret".getBytes();
        PrimosNode node = builder(1).clusterSecret(secret).build();
        java.util.Arrays.fill(secret, (byte) 0);

        assertThatCode(node::start).doesNotThrowAnyException();
        started.add(node);
    }

    // -------------------------------------------------------------- helpers

    private PrimosNode start(org.lolaf.primos.PrimosNodeBuilder builder) {
        PrimosNode node = builder.build();
        node.start();
        started.add(node);
        return node;
    }

    private org.lolaf.primos.PrimosNodeBuilder builder(int id, int... peerIds) {
        List<Peer> peers = new ArrayList<>();
        for (int peerId : peerIds) {
            peers.add(Peer.application(peerId, "loopback", 7000 + peerId));
        }
        return PrimosNode.builder()
                .applicationName("orders")
                .nodeId(id)
                .peers(peers)
                .timings(FAST)
                .store(new MemoryStateStore())
                .transport(network.transportFor(id));
    }

    private static int leaderCount(PrimosNode... nodes) {
        int leaders = 0;
        for (PrimosNode node : nodes) {
            if (node.role() == Role.LEADER) {
                leaders++;
            }
        }
        return leaders;
    }

    /** Records what a listener was told, and on which thread. */
    private static final class Recording implements PrimosListener {

        private final List<String> roles = new CopyOnWriteArrayList<>();
        private final List<String> leaders = new CopyOnWriteArrayList<>();
        private final List<String> threads = new CopyOnWriteArrayList<>();
        private final AtomicInteger peerEvents = new AtomicInteger();

        @Override
        public void onRoleChanged(Role previous, Role current, long term) {
            roles.add(previous + "->" + current);
            threads.add(Thread.currentThread().getName());
        }

        @Override
        public void onLeaderChanged(OptionalInt leaderId, long term) {
            leaders.add(leaderId.isPresent() ? String.valueOf(leaderId.getAsInt()) : "none");
        }

        @Override
        public void onPeerUp(Peer peer) {
            peerEvents.incrementAndGet();
        }

        @Override
        public void onPeerDown(Peer peer) {
            peerEvents.incrementAndGet();
        }
    }
}
