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
package org.lolaf.primos.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.search.RequiredSearch;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.DropReason;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.PrimosNodeBuilder;
import org.lolaf.primos.Timings;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.transport.LoopbackNetwork;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Every meter is registered and moves. A meter nobody increments is the same failure as a counter
 * nobody reads, which is what this module exists to fix.
 */
class PrimosMetricsTest {

    private static final Timings FAST = Timings.builder()
            .heartbeatInterval(Duration.ofMillis(20))
            .electionTimeoutMin(Duration.ofMillis(100))
            .electionTimeoutMax(Duration.ofMillis(200))
            .probeInterval(Duration.ofMillis(100))
            .build();

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final LoopbackNetwork network = new LoopbackNetwork();
    private final List<PrimosNode> started = new ArrayList<>();

    @AfterEach
    void stopEverything() {
        started.forEach(PrimosNode::stop);
    }

    @Test
    void a_leader_reports_one_so_a_cluster_can_be_summed() {
        ClusterConfig cluster = cluster(1);
        PrimosMetrics metrics = PrimosMetrics.create(registry, cluster);
        PrimosNode node = start(builder(cluster).statsListener(metrics));
        metrics.bind(node);

        await().atMost(5, TimeUnit.SECONDS).until(() -> gauge("primos.leader") == 1.0);

        assertThat(gauge("primos.role", "role", "leader")).isEqualTo(1.0);
        assertThat(gauge("primos.role", "role", "follower")).isZero();
        assertThat(gauge("primos.term")).isGreaterThan(0.0);
        assertThat(gauge("primos.node.running")).isEqualTo(1.0);
        assertThat(gauge("primos.voters")).isEqualTo(1.0);
        assertThat(gauge("primos.quorum.required")).isEqualTo(1.0);

        assertThat(timerCount("primos.campaign", "outcome", "won")).isEqualTo(1);
        assertThat(counter("primos.role.changes", "to", "leader")).isEqualTo(1.0);
        assertThat(counter("primos.leader.changes")).isGreaterThanOrEqualTo(1.0);
    }

    /**
     * The counters and timers are driven through the SPI directly, which is the production path
     * for this class: a node calls exactly these methods, and there is nothing between it and them.
     */
    @Test
    void every_pushed_counter_and_timer_moves() {
        PrimosMetrics metrics = PrimosMetrics.create(registry, cluster(1, 2));

        metrics.onElectionWon(4, 1_000_000);
        metrics.onElectionLost(5, 2_000_000);
        metrics.onLeaderKnown(5, 3_000_000);
        metrics.onLeadershipRelinquished(5);
        metrics.onHandoverSent(2);
        metrics.onHandoverAccepted(6);
        metrics.onDepartureObserved(2);
        metrics.onProbeRoundTrip(2, 250_000);
        metrics.onTimingMismatch(2);
        metrics.onUnusableNodeConfig(2);
        metrics.onPeerForgotten(2);
        for (DropReason reason : DropReason.values()) {
            metrics.onDatagramDropped(reason);
        }

        assertThat(timerCount("primos.campaign", "outcome", "won")).isEqualTo(1);
        assertThat(timerCount("primos.campaign", "outcome", "lost")).isEqualTo(1);
        assertThat(timerCount("primos.leaderless")).isEqualTo(1);
        assertThat(counter("primos.leadership.relinquished")).isEqualTo(1.0);
        assertThat(counter("primos.handover", "direction", "sent")).isEqualTo(1.0);
        assertThat(counter("primos.handover", "direction", "accepted")).isEqualTo(1.0);
        assertThat(counter("primos.departures.observed", "peer", "2")).isEqualTo(1.0);
        assertThat(timerCount("primos.probe.rtt", "peer", "2")).isEqualTo(1);
        assertThat(counter("primos.timing.mismatches", "peer", "2")).isEqualTo(1.0);
        assertThat(counter("primos.node.config.unusable", "peer", "2")).isEqualTo(1.0);
        assertThat(counter("primos.peers.forgotten")).isEqualTo(1.0);
        for (DropReason reason : DropReason.values()) {
            assertThat(counter("primos.datagrams.dropped", "reason", reason.name().toLowerCase()))
                    .as("drop reason %s", reason)
                    .isEqualTo(1.0);
        }
    }

    /**
     * Timers carry their own count, so there is deliberately no counter beside one: the campaign
     * timer's count <em>is</em> elections won.
     */
    @Test
    void no_counter_duplicates_a_timers_count() {
        List<String> names = new ArrayList<>();
        PrimosMetrics.create(registry, cluster(1, 2));
        for (Meter meter : registry.getMeters()) {
            names.add(meter.getId().getName());
        }

        assertThat(names).doesNotContain("primos.elections", "primos.elections.won",
                "primos.campaigns", "primos.outages");
    }

    @Test
    void peer_tags_come_from_the_configured_set_and_anything_else_is_bucketed() {
        ClusterConfig cluster = ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(1)
                .peer(Peer.application(2, "loopback", 7002))
                .witness(Peer.witness(9, "referee", 7009))
                .timings(FAST)
                .build();
        PrimosMetrics metrics = PrimosMetrics.create(registry, cluster);

        metrics.onProbeRoundTrip(2, 100_000);
        metrics.onProbeRoundTrip(9, 900_000);
        metrics.onProbeRoundTrip(77, 500_000);

        assertThat(tag("primos.probe.rtt", "peer", "2", "peer.type")).isEqualTo("application");
        assertThat(tag("primos.probe.rtt", "peer", "9", "peer.type"))
                .as("a witness sits in a third failure domain, so its round trip is a WAN number"
                        + " that must not share a histogram with the LAN ones")
                .isEqualTo("witness");
        assertThat(timerCount("primos.probe.rtt", "peer", "other"))
                .as("an id outside the configured set is bucketed rather than given a series")
                .isEqualTo(1);
    }

    @Test
    void a_witness_collapses_the_peer_tag_because_its_voters_arrive_from_the_network() {
        ClusterConfig witness = ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(9)
                .asWitness()
                .learnedMembership()
                .timings(FAST)
                .build();
        PrimosMetrics metrics = PrimosMetrics.create(registry, witness);

        metrics.onProbeRoundTrip(2, 100_000);
        metrics.onProbeRoundTrip(3, 200_000);

        assertThat(timerCount("primos.probe.rtt", "peer", "learned"))
                .as("learned ids arrive from the socket, so tagging per peer is an unbounded label"
                        + " set an attacker chooses")
                .isEqualTo(2);
        assertThat(registry.find("primos.probe.rtt").tag("peer", "2").timer()).isNull();
    }

    @Test
    void the_quorum_gauges_are_absent_when_the_voter_set_is_not_known() {
        ClusterConfig witness = ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(9)
                .asWitness()
                .learnedMembership()
                .timings(FAST)
                .build();
        PrimosMetrics metrics = PrimosMetrics.create(registry, witness);
        PrimosNode node = start(builder(witness).statsListener(metrics));
        metrics.bind(node);

        assertThat(registry.find("primos.voters").gauge())
                .as("Quorum.unknown is never reached; publishing 0 voters would invite an alert"
                        + " that the cluster had lost its members")
                .isNull();
        assertThat(gauge("primos.leader")).isZero();
    }

    @Test
    void binding_twice_is_refused_because_it_would_double_count_every_event() {
        ClusterConfig cluster = cluster(1);
        PrimosMetrics metrics = PrimosMetrics.create(registry, cluster);
        PrimosNode node = start(builder(cluster).statsListener(metrics));
        metrics.bind(node);

        assertThatThrownBy(() -> metrics.bind(node))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already bound");
    }

    /**
     * The real path: two nodes probing each other, then one leaving. Its twin is that the same
     * departure is counted separately from the peer going down, which is what tells an orderly
     * shutdown from a crash inferred out of silence.
     */
    @Test
    void a_peer_leaving_moves_its_gauge_its_counter_and_the_departure_counter() {
        ClusterConfig one = cluster(1, 2);
        PrimosMetrics metrics = PrimosMetrics.create(registry, one);
        PrimosNode node = start(builder(one).statsListener(metrics));
        metrics.bind(node);
        PrimosNode other = start(builder(cluster(2, 1)));

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> gauge("primos.peer.up", "peer", "2") == 1.0);

        other.stop();
        started.remove(other);

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> gauge("primos.peer.up", "peer", "2") == 0.0);
        assertThat(counter("primos.peer.down", "peer", "2")).isEqualTo(1.0);
        assertThat(counter("primos.departures.observed", "peer", "2"))
                .as("it announced itself rather than being inferred from silence")
                .isEqualTo(1.0);
    }

    @Test
    void a_publication_is_counted_on_the_leader_and_on_the_follower_told_of_it() {
        ClusterConfig one = cluster(1, 2);
        PrimosMetrics metrics = PrimosMetrics.create(registry, one);
        PrimosNode node = start(builder(one).statsListener(metrics));
        metrics.bind(node);
        PrimosNode other = start(builder(cluster(2, 1)));
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role().isLeader() || other.role().isLeader());
        boolean metredNodeLeads = node.role().isLeader();

        (metredNodeLeads ? node : other).publishChange().orElseThrow(AssertionError::new);

        String moved = metredNodeLeads ? "primos.versions.published" : "primos.versions.received";
        String still = metredNodeLeads ? "primos.versions.received" : "primos.versions.published";
        await().atMost(5, TimeUnit.SECONDS).until(() -> counter(moved) == 1.0);
        assertThat(counter(still)).isZero();
    }

    // ------------------------------------------------------------- helpers

    private double gauge(String name, String... tags) {
        var found = registry.find(name).tags(tags).gauge();
        return found == null ? Double.NaN : found.value();
    }

    private double counter(String name, String... tags) {
        var found = registry.find(name).tags(tags).counter();
        return found == null ? Double.NaN : found.count();
    }

    private long timerCount(String name, String... tags) {
        var found = registry.find(name).tags(tags).timer();
        return found == null ? -1 : found.count();
    }

    private String tag(String name, String tagKey, String tagValue, String wanted) {
        RequiredSearch search = registry.get(name).tag(tagKey, tagValue);
        return search.meter().getId().getTag(wanted);
    }

    private ClusterConfig cluster(int id, int... peerIds) {
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

    private PrimosNodeBuilder builder(ClusterConfig cluster) {
        return PrimosNode.builder()
                .clusterConfig(cluster)
                .store(MemoryStateStore::new)
                .transport(() -> network.transportFor(cluster.nodeId()));
    }

    private PrimosNode start(PrimosNodeBuilder builder) {
        PrimosNode node = builder.build();
        node.start();
        started.add(node);
        return node;
    }
}
