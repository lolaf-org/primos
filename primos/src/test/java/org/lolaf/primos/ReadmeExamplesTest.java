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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.store.FileStateStore;
import org.lolaf.primos.transport.UdpTransport;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The examples in README.md and docs/, compiled and run.
 *
 * <p>Documentation that does not compile is worse than none: it is read as authoritative and
 * costs an hour to disbelieve. Everything here is copied from the documentation, so an API change
 * that makes it wrong breaks the build instead of quietly misleading the next reader.
 */
class ReadmeExamplesTest {

    @TempDir
    Path directory;

    @Test
    void the_quick_start_builds_a_node() {
        byte[] secret = "a-shared-cluster-secret".getBytes(StandardCharsets.UTF_8);
        List<Peer> peers = Arrays.asList(
                Peer.application(2, "127.0.0.1", 7421),
                Peer.application(3, "127.0.0.1", 7422));

        try (PrimosNode node = PrimosNode.builder()
                .clusterConfig(ClusterConfig.builder()
                        .applicationName("orders")
                        .nodeId(1)
                        .peers(peers)
                        .build())
                .clusterSecret(secret)
                .store(() -> new FileStateStore(directory))
                .transport(UdpTransport.builder()
                        .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                        .peers(peers)
                        .addressLearning(true))
                .listener(new PrimosListener() {
                    @Override
                    public void onRoleChanged(Role previous, Role current, long term) {
                        // nothing to do
                    }
                })
                .build()) {

            node.start();
            assertThat(node.role()).isIn(Role.FOLLOWER, Role.CANDIDATE, Role.LEADER);

            // The fencing pattern from docs/using-leadership-safely.md: role and term read together.
            RoleAndTerm current = node.roleAndTerm();
            assertThat(current.isLeader()).isEqualTo(current.role() == Role.LEADER);
            assertThat(current.term()).isNotNegative();
        }
    }

    @Test
    void every_stats_event_the_readme_names_exists() {
        RecordingStats recorded = new RecordingStats();
        try (PrimosNode node = PrimosNode.builder()
                .statsListener(recorded)
                .clusterConfig(ClusterConfig.builder()
                        .applicationName("orders")
                        .nodeId(1)
                        .peer(Peer.application(2, "127.0.0.1", 7421))
                        .build())
                .store(() -> new FileStateStore(directory))
                .transport(UdpTransport.builder()
                        .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                        .peers(List.of(Peer.application(2, "127.0.0.1", 7421))))
                .build()) {

            node.start();

            assertThat(node.roleAndTerm().role()).isNotNull();
            assertThat(node.roleAndTerm().term()).isNotNegative();

            // Every event docs/observability.md's table names, so the table cannot drift from the interface.
            PrimosStatsListener every = new PrimosStatsListener() {
            };
            every.onElectionWon(1, 1);
            every.onElectionLost(1, 1);
            every.onLeaderKnown(1, 1);
            every.onLeadershipRelinquished(1);
            every.onHandoverSent(2);
            every.onHandoverAccepted(1);
            every.onDepartureObserved(2);
            every.onProbeRoundTrip(2, 1);
            every.onDatagramDropped(DropReason.BAD_TAG);
            every.onTimingMismatch(2);
            every.onUnusableNodeConfig(2);
            every.onPeerForgotten(2);

            assertThat(recorded.electionsWon.get()).isNotNegative();
            assertThat(recorded.handoversSent.get()).isZero();
            assertThat(recorded.departuresObserved.get()).isZero();
            assertThat(recorded.totalDrops()).isZero();
        }
    }

    @Test
    void the_two_timing_profiles_the_readme_tabulates_are_what_it_says() {
        Timings lan = Timings.lan();
        assertThat(lan.heartbeatInterval().toMillis()).isEqualTo(50);
        assertThat(lan.electionTimeoutMin().toMillis()).isEqualTo(300);
        assertThat(lan.probeInterval().toMillis()).isEqualTo(1_000);
        assertThat(lan.failureThreshold()).isEqualTo(3);

        Timings wan = Timings.wan();
        assertThat(wan.heartbeatInterval().toMillis()).isEqualTo(200);
        assertThat(wan.electionTimeoutMin().toMillis()).isEqualTo(1_500);
        assertThat(wan.probeInterval().toMillis()).isEqualTo(2_000);
    }

    @Test
    void the_derived_profile_the_readme_shows_is_accepted() {
        Timings custom = Timings.builder()
                .heartbeatInterval(java.time.Duration.ofMillis(200))
                .electionTimeoutMin(java.time.Duration.ofMillis(1_500))
                .electionTimeoutMax(java.time.Duration.ofMillis(3_000))
                .probeInterval(java.time.Duration.ofSeconds(2))
                .failureThreshold(3)
                .build();

        assertThat(custom).isEqualTo(Timings.wan());
    }

    @Test
    void build_rejects_exactly_what_the_readme_says_it_rejects() {
        // Each of these is a line in docs/timings.md's "Deriving your own" section. If a rule changes,
        // the documentation is wrong and this fails rather than the reader finding out later.
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> Timings.builder()
                        .heartbeatInterval(java.time.Duration.ofMillis(100))
                        .electionTimeoutMin(java.time.Duration.ofMillis(400))
                        .electionTimeoutMax(java.time.Duration.ofMillis(800))
                        .build())
                .as("an election timeout under five heartbeats")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("5 heartbeat intervals");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> Timings.builder()
                        .electionTimeoutMin(java.time.Duration.ofMillis(300))
                        .electionTimeoutMax(java.time.Duration.ofMillis(320))
                        .build())
                .as("a randomisation range narrower than one heartbeat")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("split the vote");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> Timings.builder()
                        .probeInterval(java.time.Duration.ofSeconds(16))
                        .build())
                .as("a probe interval past the NAT pinhole ceiling")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NAT pinhole");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> Timings.builder()
                        .failureThreshold(0)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureThreshold must be at least 1");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> Timings.builder()
                        .heartbeatInterval(java.time.Duration.ZERO)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void a_wan_witness_in_a_lan_cluster_is_five_times_slower_to_agree_to_a_failover() {
        // docs/timings.md tells operators to use one profile cluster-wide. This is the number behind
        // that: a node refuses a pre-vote while it has heard from a leader within
        // electionTimeoutMin, so the witness's own profile decides when it will agree to a
        // failover — which, for a two-node pair, is the vote everything is waiting on.
        assertThat(Timings.wan().electionTimeoutMin())
                .isEqualTo(Timings.lan().electionTimeoutMin().multipliedBy(5));
    }

    @Test
    void a_node_without_a_store_or_a_transport_is_refused_as_the_readme_says() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> PrimosNode.builder()
                        .clusterConfig(ClusterConfig.builder()
                                .applicationName("orders")
                                .nodeId(1)
                                .peer(Peer.application(2, "127.0.0.1", 7421))
                                .build())
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("state store is required");
    }

    // ------------------------------------------------- docs/leader-data.md

    @Test
    void the_leader_data_example_publishes_and_a_follower_applies_only_the_matching_term() {
        try (PrimosNode node = PrimosNode.builder()
                .clusterConfig(ClusterConfig.builder().applicationName("orders").nodeId(1).build())
                .store(() -> new FileStateStore(directory))
                .transport(UdpTransport.builder().bindAddress(new InetSocketAddress("127.0.0.1", 0)))
                .build()) {
            AssignmentPublisher publisher = new AssignmentPublisher();
            publisher.attachTo(node);
            node.start();

            await().atMost(5, TimeUnit.SECONDS).until(() -> publisher.current() != null);
            Edition published = publisher.current();
            assertThat(published.term).isEqualTo(node.roleAndTerm().term());
            assertThat(published.version).isEqualTo(1);

            Peer leaderAt = Peer.application(1, "127.0.0.1", 7420);
            AssignmentFollower stale = new AssignmentFollower(host -> published);
            stale.fetch(leaderAt, published.term + 1, 1);
            assertThat(stale.applied).as("fetched data from another term is not the current leader's").isNull();

            AssignmentFollower follower = new AssignmentFollower(host -> published);
            follower.onLeaderVersion(Leader.at(leaderAt), published.term, published.version);
            await().atMost(5, TimeUnit.SECONDS).until(() -> follower.applied == published);
        }
    }

    interface Fetcher {
        Edition fetch(String host);
    }

    static final class Assignment {
        static Assignment compute() {
            return new Assignment();
        }
    }

    static final class Edition {
        final long term;
        final long version;
        final Assignment assignment;

        Edition(long term, long version, Assignment assignment) {
            this.term = term;
            this.version = version;
            this.assignment = assignment;
        }
    }

    static class AssignmentPublisher implements PrimosListener {

        private final AtomicReference<Edition> served = new AtomicReference<>();
        private final ExecutorService work = Executors.newSingleThreadExecutor();
        private PrimosNode node;

        void attachTo(PrimosNode node) {
            this.node = node;
            node.addListener(this);
        }

        @Override
        public void onRoleChanged(Role previous, Role current, long term) {
            if (current == Role.LEADER) {
                work.submit(this::rebalance);
            }
        }

        void rebalance() {
            Assignment next = Assignment.compute();
            node.publishChange().ifPresent(published ->
                    served.set(new Edition(published.term(), published.version(), next)));
        }

        Edition current() {
            return served.get();
        }
    }

    static class AssignmentFollower implements PrimosListener {

        private final ExecutorService work = Executors.newSingleThreadExecutor();
        private final Fetcher fetcher;
        private volatile Edition applied;

        AssignmentFollower(Fetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public void onLeaderVersion(Leader leader, long term, long version) {
            if (leader.isSelf()) {
                return;
            }
            leader.peer().ifPresent(at -> work.submit(() -> fetch(at, term, version)));
        }

        void fetch(Peer leader, long term, long version) {
            Edition edition = fetcher.fetch(leader.host());
            if (edition == null || edition.term != term || edition.version < version) {
                return;
            }
            applied = edition;
        }
    }
}
