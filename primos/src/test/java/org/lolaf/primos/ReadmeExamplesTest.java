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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.store.FileStateStore;
import org.lolaf.primos.transport.UdpTransport;

/**
 * The README's examples, compiled and run.
 *
 * <p>Documentation that does not compile is worse than none: it is read as authoritative and
 * costs an hour to disbelieve. Everything here is copied from README.md, so an API change that
 * makes the README wrong breaks the build instead of quietly misleading the next reader.
 */
class ReadmeExamplesTest {

    @TempDir
    Path directory;

    @Test
    void the_quick_start_builds_a_node() throws IOException {
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
                         }
                     })
                     .build()) {

            node.start();
            assertThat(node.role()).isIn(Role.FOLLOWER, Role.CANDIDATE, Role.LEADER);

            // The fencing pattern from "Using it safely": role and term read together.
            RoleAndTerm current = node.roleAndTerm();
            assertThat(current.isLeader()).isEqualTo(current.role() == Role.LEADER);
            assertThat(current.term()).isNotNegative();
        }
    }

    @Test
    void every_stats_event_the_readme_names_exists() throws IOException {
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
                             .peers(Arrays.asList(Peer.application(2, "127.0.0.1", 7421))))
                     .build()) {

            node.start();

            assertThat(node.roleAndTerm().role()).isNotNull();
            assertThat(node.roleAndTerm().term()).isNotNegative();
            assertThat(recorded.electionsWon.get()).isNotNegative();
            assertThat(recorded.handoversSent.get()).isZero();
            assertThat(recorded.handoversAccepted.get()).isZero();
            assertThat(recorded.departuresObserved.get()).isZero();
            assertThat(recorded.drops(DropReason.BAD_TAG)).isZero();
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
        // Each of these is a line in the README's "Deriving your own" section. If a rule changes,
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
        // The README tells operators to use one profile cluster-wide. This is the number behind
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
}
