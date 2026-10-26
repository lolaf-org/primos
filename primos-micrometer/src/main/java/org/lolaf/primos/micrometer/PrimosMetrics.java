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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.DropReason;
import org.lolaf.primos.Leader;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PeerDownReason;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.PrimosStatsListener;
import org.lolaf.primos.Quorum;
import org.lolaf.primos.Role;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A node's meters, on whatever {@link MeterRegistry} the application already runs.
 *
 * <p><strong>It is used in two halves, and they are not the same call because they cannot be.</strong>
 * The counters and timers arrive through {@link PrimosStatsListener}, which is one sink fixed at
 * build time, so this object has to exist <em>before</em> the node does. The gauges poll the node,
 * so they cannot be registered until it exists. Hence:
 *
 * <pre>
 * PrimosMetrics metrics = PrimosMetrics.create(registry, cluster);
 *
 * PrimosNode node = PrimosNode.builder()
 *         .clusterConfig(cluster)
 *         .statsListener(metrics)
 *         .store(stores)
 *         .transport(transports)
 *         .build();
 *
 * metrics.bind(node);
 * </pre>
 *
 * <p>{@link #bind} also registers for role, leader and peer events, which is why there is nothing
 * to pass to {@code listener(...)}: those may be added after {@code start()}, because
 * {@link PrimosNode#addListener} replays the current state to a late listener.
 *
 * <h2>Tags</h2>
 *
 * <p>Every meter carries {@code application} and {@code node}. One JVM runs several nodes in tests
 * and one node per served cluster in the witness server, so untagged meters would silently sum
 * across clusters. Meters that concern one peer also carry {@code peer} and {@code peer.type};
 * see {@link #create} for the bound on what those can be.
 *
 * <p>The meter to alert on is <strong>{@code primos.leader}</strong>: summed across a cluster it is
 * 1 when things are well, 0 during an election, and 2 when this library has failed at its one job.
 */
public final class PrimosMetrics implements PrimosStatsListener {

    private static final String PEER = "peer";
    private static final String PEER_TYPE = "peer.type";
    private static final String HANDOVER = "Leadership handed over: sent by a leader shutting down, transferred by"
            + " one staying in the cluster, or accepted from either";

    private final MeterRegistry registry;
    private final Tags base;
    private final boolean learnedMembership;

    /**
     * The configured peer ids, in declaration order, with one extra slot past the end for
     * everything else. Scanned rather than hashed: a handful of voters, on a path that must not
     * allocate, which is the same trade {@code ElectionNode} makes for the same reason.
     */
    private final int[] peerIds;
    private final Tags[] peerTags;

    private final AtomicInteger[] peerUp;
    private final Timer[] probeRtt;
    private final Counter[] peerDown;
    private final Counter[] departures;
    private final Counter[] timingMismatches;
    private final Counter[] unusableNodeConfig;

    private final Timer campaignWon;
    private final Timer campaignLost;
    private final Timer leaderless;
    private final Counter relinquished;
    private final Counter handoverSent;
    private final Counter handoverTransferred;
    private final Counter handoverAccepted;
    private final Counter noSuccessor;
    private final Counter peersForgotten;
    private final Counter leaderChanges;
    private final Counter versionsPublished;
    private final Counter versionsReceived;
    private final Counter[] dropped;
    private final Counter[] roleChanges;

    private final AtomicBoolean bound = new AtomicBoolean();

    /**
     * Meters for one node of one cluster.
     *
     * <p>The peer tag is resolved from the configured peer set here, once, and an id that is not in
     * it is counted under {@code peer=other}. A witness server learns its voters from traffic, so
     * there is no configured set to resolve against and every peer-scoped meter collapses to
     * {@code peer=learned} — an unbounded label set driven by whatever arrives on the socket is the
     * metrics version of a memory leak an attacker controls.
     */
    public static PrimosMetrics create(MeterRegistry registry, ClusterConfig cluster) {
        Objects.requireNonNull(cluster, "cluster");
        boolean learned = cluster.isLearnedMembership();
        return new PrimosMetrics(registry, cluster.applicationName(), cluster.nodeId(),
                learned ? Collections.emptyList() : cluster.allPeers(), learned);
    }

    static PrimosMetrics forServedApplication(MeterRegistry registry, String application, int nodeId) {
        return new PrimosMetrics(registry, application, nodeId, Collections.emptyList(), true);
    }

    private PrimosMetrics(MeterRegistry registry, String application, int nodeId,
                          List<Peer> peers, boolean learnedMembership) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.base = Tags.of("application", application, "node", String.valueOf(nodeId));
        this.learnedMembership = learnedMembership;

        int slots = peers.size() + 1;
        this.peerIds = new int[peers.size()];
        this.peerTags = new Tags[slots];
        for (int i = 0; i < peers.size(); i++) {
            Peer peer = peers.get(i);
            peerIds[i] = peer.id();
            peerTags[i] = base.and(PEER, String.valueOf(peer.id()))
                    .and(PEER_TYPE, peer.isWitness() ? "witness" : "application");
        }
        peerTags[slots - 1] = base.and(PEER, learnedMembership ? "learned" : "other")
                .and(PEER_TYPE, "unknown");

        this.peerUp = new AtomicInteger[peers.size()];
        this.probeRtt = new Timer[slots];
        this.peerDown = new Counter[slots];
        this.departures = new Counter[slots];
        this.timingMismatches = new Counter[slots];
        this.unusableNodeConfig = new Counter[slots];
        for (int i = 0; i < slots; i++) {
            if (i < peerUp.length) {
                peerUp[i] = new AtomicInteger();
            }
            probeRtt[i] = Timer.builder("primos.probe.rtt")
                    .description("Round trip to one peer, every sample, which is what a percentile needs")
                    .tags(peerTags[i]).register(registry);
            peerDown[i] = counter("primos.peer.down",
                    "Times a peer stopped answering probes", peerTags[i]);
            departures[i] = counter("primos.departures.observed",
                    "Peers that said they were shutting down, rather than being inferred from silence",
                    peerTags[i]);
            timingMismatches[i] = counter("primos.timing.mismatches",
                    "Times a peer was seen running different intervals from this node's", peerTags[i]);
            unusableNodeConfig[i] = counter("primos.node.config.unusable",
                    "Times a peer stated intervals that could not form a valid profile", peerTags[i]);
        }

        this.campaignWon = campaign("won");
        this.campaignLost = campaign("lost");
        this.leaderless = Timer.builder("primos.leaderless")
                .description("How long this node had no leader, spanning the whole outage rather than one election")
                .tags(base).register(registry);
        this.relinquished = counter("primos.leadership.relinquished",
                "Times this node stood itself down for want of contact with a majority", base);
        this.handoverSent = counter("primos.handover", HANDOVER, base.and("direction", "sent"));
        this.handoverTransferred = counter("primos.handover", HANDOVER, base.and("direction", "transferred"));
        this.handoverAccepted = counter("primos.handover", HANDOVER, base.and("direction", "accepted"));
        this.noSuccessor = counter("primos.handover.no.successor",
                "Transfers that found no follower standing for election to hand leadership to", base);
        this.peersForgotten = counter("primos.peers.forgotten",
                "Learned voters dropped after going quiet. Always zero with a configured peer set", base);
        this.leaderChanges = counter("primos.leader.changes",
                "Leader changes seen by this node, including losing one", base);
        this.versionsPublished = counter("primos.versions.published",
                "Versions this node published as leader; several between two heartbeats count once", base);
        this.versionsReceived = counter("primos.versions.received",
                "Leader versions this node was told about as a follower", base);

        this.dropped = new Counter[DropReason.values().length];
        for (DropReason reason : DropReason.values()) {
            dropped[reason.ordinal()] = counter("primos.datagrams.dropped",
                    "Datagrams discarded, by the check that discarded them",
                    base.and("reason", lower(reason.name())));
        }
        this.roleChanges = new Counter[Role.values().length];
        for (Role role : Role.values()) {
            roleChanges[role.ordinal()] = counter("primos.role.changes",
                    "Role transitions, by the role moved into", base.and("to", lower(role.name())));
        }
    }

    /**
     * Registers the gauges, and starts counting role, leader and peer events.
     *
     * @throws IllegalStateException if called twice, which would double-count every event
     */
    public void bind(PrimosNode node) {
        Objects.requireNonNull(node, "node");
        if (!bound.compareAndSet(false, true)) {
            throw new IllegalStateException("these metrics are already bound to a node");
        }

        gauge("primos.leader", node, n -> n.role().isLeader() ? 1 : 0,
                "1 while this node is leader. Summed across a cluster: 1 when well, 0 during an"
                        + " election, 2 when this library has failed at its one job", base);
        for (Role role : Role.values()) {
            gauge("primos.role", node, n -> n.role() == role ? 1 : 0,
                    "1 while the node holds this role", base.and("role", lower(role.name())));
        }
        gauge("primos.term", node, n -> n.roleAndTerm().term(),
                "The term this node is in. The fencing token", base);
        gauge("primos.node.running", node, n -> n.isRunning() ? 1 : 0,
                "1 while the node is started. Separates a node nobody started from a healthy follower",
                base);
        gauge("primos.candidate", node, n -> n.isCandidate() ? 1 : 0,
                "1 while this node stands for election. Summed across a cluster: 0 means nobody can"
                        + " take over from the leader", base);

        Quorum quorum = Quorum.of(node.config());
        if (quorum.isKnown()) {
            gauge("primos.voters", quorum, Quorum::voters, "Voters in the configured cluster", base);
            gauge("primos.quorum.required", quorum, Quorum::required,
                    "Votes needed to lead. Against the peers that are up, this is how close the"
                            + " cluster is to having no leader at all", base);
        }
        for (int i = 0; i < peerUp.length; i++) {
            gauge("primos.peer.up", peerUp[i], AtomicInteger::get,
                    "1 while a peer answers probes. 0 until its first reply after binding",
                    peerTags[i]);
        }

        node.addListener(new Events());
    }

    @Override
    public void onElectionWon(long term, long campaignNanos) {
        campaignWon.record(campaignNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onElectionLost(long term, long campaignNanos) {
        campaignLost.record(campaignNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onLeaderKnown(long term, long leaderlessNanos) {
        leaderless.record(leaderlessNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onLeadershipRelinquished(long term) {
        relinquished.increment();
    }

    @Override
    public void onHandoverSent(int toPeerId) {
        handoverSent.increment();
    }

    @Override
    public void onLeadershipTransferSent(int toPeerId) {
        handoverTransferred.increment();
    }

    @Override
    public void onNoSuccessor(long term) {
        noSuccessor.increment();
    }

    @Override
    public void onHandoverAccepted(long term) {
        handoverAccepted.increment();
    }

    @Override
    public void onDepartureObserved(int peerId) {
        departures[slot(peerId)].increment();
    }

    @Override
    public void onProbeRoundTrip(int peerId, long nanos) {
        probeRtt[slot(peerId)].record(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onDatagramDropped(DropReason reason) {
        dropped[reason.ordinal()].increment();
    }

    @Override
    public void onTimingMismatch(int peerId) {
        timingMismatches[slot(peerId)].increment();
    }

    @Override
    public void onUnusableNodeConfig(int peerId) {
        unusableNodeConfig[slot(peerId)].increment();
    }

    @Override
    public void onPeerForgotten(int peerId) {
        peersForgotten.increment();
    }

    /**
     * Which set of peer-tagged meters an id belongs to, or the bucket past the end.
     */
    private int slot(int peerId) {
        for (int i = 0; i < peerIds.length; i++) {
            if (peerIds[i] == peerId) {
                return i;
            }
        }
        return peerIds.length;
    }

    private Timer campaign(String outcome) {
        return Timer.builder("primos.campaign")
                .description("How long a campaign took, measured from the pre-vote, which is the"
                        + " question an operator is asking after a failover")
                .tags(base.and("outcome", outcome))
                .register(registry);
    }

    private Counter counter(String name, String description, Tags tags) {
        return Counter.builder(name).description(description).tags(tags).register(registry);
    }

    /**
     * A gauge holding its subject strongly. Micrometer's default is a weak reference, which is
     * right for a cache and wrong here: nothing else need hold this object, and a collected gauge
     * reports NaN forever with nothing to say why.
     */
    private <T> void gauge(String name, T subject, java.util.function.ToDoubleFunction<T> value,
                           String description, Tags tags) {
        Gauge.builder(name, subject, value)
                .description(description)
                .tags(tags)
                .strongReference(true)
                .register(registry);
    }

    private static String lower(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    boolean isLearnedMembership() {
        return learnedMembership;
    }

    /**
     * The half of the surface that {@link PrimosListener} delivers. Kept off the public type so
     * that it can only be registered by {@link #bind}, which is what stops it being counted twice
     * where both halves are wired by a container.
     */
    private final class Events implements PrimosListener {

        @Override
        public void onRoleChanged(Role previous, Role current, long term) {
            roleChanges[current.ordinal()].increment();
        }

        @Override
        public void onLeaderChanged(Optional<Leader> leader, long term) {
            leaderChanges.increment();
        }

        @Override
        public void onLeaderVersion(Leader leader, long term, long version) {
            (leader.isSelf() ? versionsPublished : versionsReceived).increment();
        }

        @Override
        public void onPeerUp(Peer peer) {
            int slot = slot(peer.id());
            if (slot < peerUp.length) {
                peerUp[slot].set(1);
            }
        }

        @Override
        public void onPeerDown(Peer peer, PeerDownReason reason) {
            int slot = slot(peer.id());
            if (slot < peerUp.length) {
                peerUp[slot].set(0);
            }
            peerDown[slot].increment();
        }
    }
}
