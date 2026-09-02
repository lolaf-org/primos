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
import org.lolaf.primos.transport.LoopbackNetwork;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * The public API, running for real: its own thread, its own socket, its own clock.
 */
class PrimosNodeTest {

    private static final Timings FAST = Timings.builder()
            .heartbeatInterval(Duration.ofMillis(20))
            .electionTimeoutMin(Duration.ofMillis(100))
            .electionTimeoutMax(Duration.ofMillis(200))
            .probeInterval(Duration.ofMillis(100))
            .build();

    private final LoopbackNetwork network = new LoopbackNetwork();
    private final List<PrimosNode> started = new ArrayList<>();
    private ExecutorService executor;

    private static ClusterConfig cluster(int id, int... peerIds) {
        List<Peer> peers = new ArrayList<>();
        for (int peerId : peerIds) {
            peers.add(Peer.application(peerId, "loopback", 7000 + peerId));
        }
        return ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(id)
                .peers(peers)
                .timings(FAST)
                .build();
    }

    // ------------------------------------------------------------- lifecycle

    private static int leaderCount(PrimosNode... nodes) {
        int leaders = 0;
        for (PrimosNode node : nodes) {
            if (node.role().isLeader()) {
                leaders++;
            }
        }
        return leaders;
    }

    @AfterEach
    void stopEverything() {
        started.forEach(PrimosNode::stop);
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void a_single_node_leads_itself() {
        PrimosNode node = start(builder(1));

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());
        assertThat(node.roleAndTerm().isLeader()).isTrue();

        Leader leader = node.leader().orElseThrow(AssertionError::new);
        assertThat(leader.id()).isEqualTo(1);
        assertThat(leader.isSelf())
                .as("leading itself is present and says so; empty would mean nobody leads")
                .isTrue();
        assertThat(leader.peer()).isEmpty();
    }

    @Test
    void start_returns_before_a_leader_exists_rather_than_pretending_otherwise() {
        PrimosNode node = start(builder(1, 2, 3));

        assertThat(node.role())
                .as("an election takes at least one election timeout; start() must not block for it")
                .isEqualTo(Role.FOLLOWER);
    }

    /**
     * Idempotent, like {@code stop()}. Both are reached from lifecycle callbacks and shutdown
     * hooks, where throwing would turn an ordering the caller cannot see into a failure.
     */
    @Test
    void starting_twice_is_a_no_op() {
        PrimosNode node = start(builder(1));
        PrimosStats before = node.stats();

        node.start();

        assertThat(node.isRunning()).isTrue();
        assertThat(node.stats().timeInRole())
                .as("the running node was left alone, not replaced")
                .isGreaterThanOrEqualTo(before.timeInRole());
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
    void a_stopped_node_stops_claiming_leadership() {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());

        node.stop();

        assertThat(node.role())
                .as("an application that checks after shutdown must not be told it is in charge")
                .isNotEqualTo(Role.LEADER);
        assertThat(node.roleAndTerm().isLeader()).isFalse();
        assertThat(node.leader()).isEmpty();
    }

    @Test
    void stopping_tells_the_listeners_that_leadership_is_over() {
        Recording events = new Recording();
        PrimosNode node = start(builder(1).listener(events));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());

        node.stop();

        assertThat(events.roles).endsWith("LEADER->FOLLOWER");
        assertThat(events.leaders).endsWith("none");
    }

    // ------------------------------------------------------------- a cluster

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

    @Test
    void three_real_nodes_on_three_real_threads_elect_one_leader() {
        PrimosNode one = start(builder(1, 2, 3));
        PrimosNode two = start(builder(2, 1, 3));
        PrimosNode three = start(builder(3, 1, 2));

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> leaderCount(one, two, three) == 1);

        assertThat(leaderCount(one, two, three)).isEqualTo(1);
    }

    // ------------------------------------------------------------- listeners

    @Test
    void the_followers_learn_who_leads() {
        PrimosNode one = start(builder(1, 2, 3));
        PrimosNode two = start(builder(2, 1, 3));
        PrimosNode three = start(builder(3, 1, 2));

        await().atMost(10, TimeUnit.SECONDS).until(() ->
                one.leader().isPresent() && two.leader().isPresent()
                        && three.leader().isPresent());

        int leaderId = one.leader().orElseThrow(AssertionError::new).id();
        assertThat(two.leader()).map(Leader::id).hasValue(leaderId);
        assertThat(three.leader()).map(Leader::id).hasValue(leaderId);

        // The nodes agree on who leads, not on the object describing it: whichever node holds it
        // reports itself, and the other two report a peer they can forward work to.
        int selves = 0;
        for (PrimosNode node : new PrimosNode[]{one, two, three}) {
            Leader leader = node.leader().orElseThrow(AssertionError::new);
            if (leader.isSelf()) {
                selves++;
            } else {
                assertThat(leader.peer()).map(Peer::id).hasValue(leaderId);
            }
        }
        assertThat(selves).isEqualTo(1);
    }

    @Test
    void role_is_answerable_from_any_thread() throws Exception {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());

        executor = Executors.newSingleThreadExecutor();
        Role fromElsewhere = executor.submit(node::role).get(5, TimeUnit.SECONDS);

        assertThat(fromElsewhere).isEqualTo(Role.LEADER);
    }

    @Test
    void a_listener_registered_before_start_sees_the_first_transition() {
        Recording events = new Recording();
        PrimosNode node = start(builder(1).listener(events));

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());
        await().atMost(5, TimeUnit.SECONDS).until(() -> events.roles.size() >= 2);

        assertThat(events.roles)
                .as("every transition is reported, not just the final state")
                .containsExactly("FOLLOWER->CANDIDATE", "CANDIDATE->LEADER");
        assertThat(events.leaders).contains("1");
    }

    @Test
    void a_listener_added_after_start_is_told_the_state_it_missed() {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());

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

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());
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

    // ---------------------------------------------------------------- stats

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

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());
        assertThat(node.role()).isEqualTo(Role.LEADER);
    }

    @Test
    void stats_report_what_the_node_has_been_doing() {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.stats().electionsWon() > 0);

        assertThat(node.stats().role()).isEqualTo(Role.LEADER);
        assertThat(node.stats().currentTerm()).isPositive();
        assertThat(node.stats().electionsStarted()).isPositive();
        assertThat(node.stats().timeInRole()).isGreaterThanOrEqualTo(Duration.ZERO);
        assertThat(node.stats().drops()).containsKey(DropReason.BAD_TAG);
        assertThat(node.stats().totalDrops()).isZero();
    }

    @Test
    void stats_are_taken_when_they_are_asked_for() throws Exception {
        PrimosNode node = start(builder(1));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader());

        PrimosStats first = node.stats();
        Thread.sleep(100);
        PrimosStats second = node.stats();

        // Snapshots are no longer taken after every turn of the loop, so this is the assertion that
        // asking still gets a fresh one rather than whatever the loop last happened to publish.
        assertThat(second.timeInRole()).isGreaterThan(first.timeInRole());
    }

    // -------------------------------------------------------------- builder

    @Test
    void stats_can_be_read_from_inside_a_callback() {
        AtomicReference<Role> seen = new AtomicReference<>();
        PrimosNode[] node = new PrimosNode[1];
        node[0] = start(builder(1).listener(new PrimosListener() {
            @Override
            public void onRoleChanged(Role previous, Role current, long term) {
                if (current == Role.LEADER) {
                    // A callback runs on the node thread, mid-turn. Reading stats there is allowed
                    // and reports what the node is doing now, not what it was doing last turn.
                    seen.set(node[0].stats().role());
                }
            }
        }));

        await().atMost(5, TimeUnit.SECONDS).until(() -> seen.get() != null);
        assertThat(seen.get()).isEqualTo(Role.LEADER);
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

    @Test
    void a_node_without_a_store_is_refused_with_the_reason() {
        assertThatThrownBy(() -> PrimosNode.builder()
                .clusterConfig(cluster(1))
                .transport(() -> network.transportFor(1))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("state store is required")
                .hasMessageContaining("elect two leaders");
    }

    @Test
    void an_unknown_mac_algorithm_is_refused_at_build_time() {
        // Better here than as a cluster that comes up, talks to nobody, and counts BAD_TAG.
        assertThatThrownBy(() -> PrimosNode.builder()
                .clusterConfig(cluster(1))
                .transport(() -> network.transportFor(1))
                .store(MemoryStateStore::new)
                .clusterSecret("a-secret".getBytes())
                .macAlgorithm("HmacNoSuchThing")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HmacNoSuchThing");
    }

    @Test
    void a_cluster_can_authenticate_with_another_algorithm() {
        byte[] secret = "a-shared-cluster-secret".getBytes();
        PrimosNode one = start(builder(1, 2)
                .clusterSecret(secret).macAlgorithm("HmacSHA512"));
        start(builder(2, 1).clusterSecret(secret).macAlgorithm("HmacSHA512"));

        await().atMost(10, TimeUnit.SECONDS).until(() -> one.roleAndTerm().term() > 0);

        assertThat(one.stats().totalDrops())
                .as("a 64-byte tag is longer, and otherwise no different")
                .isZero();
    }

    @Test
    void a_node_without_a_transport_is_refused() {
        assertThatThrownBy(() -> PrimosNode.builder()
                .clusterConfig(cluster(1))
                .store(MemoryStateStore::new)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transport is required");
    }

    @Test
    void a_node_without_a_cluster_configuration_is_refused() {
        assertThatThrownBy(() -> PrimosNode.builder()
                .store(MemoryStateStore::new)
                .transport(() -> network.transportFor(1))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster configuration is required");
    }

    // -------------------------------------------------------------- helpers

    @Test
    void the_configuration_rules_are_applied_where_the_configuration_is_built() {
        // No longer this builder's job, and worth a test saying so: the cluster's shape is
        // validated once, by ClusterConfig, rather than at whichever of two entry points was used.
        assertThatThrownBy(() -> ClusterConfig.builder()
                .applicationName("a name with spaces").nodeId(1).build())
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

    // ---------------------------------------------------------- restartability

    /**
     * A restart must not be a way to bring a broken node back into a vote. FAILED means something
     * this node depends on is broken; rejoining would not fix it and would put a node with, say, an
     * unwritable store back where its answers count.
     */
    @Test
    void a_failed_node_is_not_restartable() {
        PrimosNode node = PrimosNode.builder()
                .clusterConfig(cluster(1))
                .store(FailingStore::new)
                .transport(() -> network.transportFor(1))
                .build();
        started.add(node);

        node.start();
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == Role.FAILED);
        node.stop();

        assertThatThrownBy(node::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has failed and cannot be restarted");
    }

    /** Persists nothing and refuses, which drives the node into FAILED through its own loop. */
    private static final class FailingStore implements StateStore {

        @Override
        public org.lolaf.primos.store.PersistentState load() {
            return new org.lolaf.primos.store.PersistentState(0, -1);
        }

        @Override
        public void save(long currentTerm, int votedFor) {
            throw new org.lolaf.primos.store.StateStoreException("this disk is broken");
        }

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public void close() {
        }
    }

    private PrimosNode nodeSharing(int id, StateStore store) {
        return PrimosNode.builder()
                .clusterConfig(cluster(id))
                .store(() -> store)
                .transport(() -> network.transportFor(id))
                .build();
    }

    private PrimosNode start(org.lolaf.primos.PrimosNodeBuilder builder) {
        PrimosNode node = builder.build();
        node.start();
        started.add(node);
        return node;
    }

    private org.lolaf.primos.PrimosNodeBuilder builder(int id, int... peerIds) {
        return PrimosNode.builder()
                .clusterConfig(cluster(id, peerIds))
                .store(MemoryStateStore::new)
                .transport(() -> network.transportFor(id));
    }

    /**
     * Records what a listener was told, and on which thread.
     */
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
        public void onLeaderChanged(Optional<Leader> leader, long term) {
            leaders.add(leader.map(l -> String.valueOf(l.id())).orElse("none"));
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
