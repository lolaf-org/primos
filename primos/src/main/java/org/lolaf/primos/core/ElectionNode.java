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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.*;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.DatagramHandler;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The Raft role state machine: terms, votes, election timeouts and heartbeats.
 *
 * <p>Implemented against figure 2 of the Raft paper, keeping its field names, minus everything
 * downstream of a log. This class has no thread of its own and no blocking of its own: it is
 * driven by repeated calls to {@link #runOnce}, which the production loop makes from one thread
 * and the deterministic simulation makes from the harness. That is what lets thousands of seeded
 * runs full of partitions and crashes exercise exactly the code that runs in production.
 *
 * <p>Two rules govern everything here, and both are safety rather than tidiness:
 *
 * <ul>
 *   <li><strong>Persist before acting.</strong> {@code currentTerm} and {@code votedFor} reach the
 *       store before any message that depends on them is sent. A node that grants a vote and then
 *       forgets it can grant a second in the same term, and two candidates each collect a
 *       majority.</li>
 *   <li><strong>Count votes in a set, never a counter.</strong> UDP duplicates datagrams, so the
 *       same grant can arrive more than once. A counter would let one voter elect a leader.</li>
 * </ul>
 *
 * <p>Not thread-safe, and not meant to be: one node, one thread.
 */
@Slf4j
public final class ElectionNode implements MessageHandler {

    /**
     * No round trip has been recorded with a peer. {@link Long#MIN_VALUE} rather than -1 because
     * these are {@link MonotonicClock#nanoTime()} readings, which may legitimately be negative.
     */
    private static final long NO_CONTACT = Long.MIN_VALUE;
    /**
     * Probe intervals of silence after which a witness stops tracking a learned voter.
     */
    private static final int FORGET_AFTER_PROBES = 60;
    /**
     * How many voters a witness will track for one application before the quietest makes room.
     */
    private static final int MAX_LEARNED_PEERS = 256;
    @Getter
    private final ClusterConfig config;
    private final Timings timings;
    private final Quorum quorum;
    private final StateStore store;
    private final Transport transport;
    private final MonotonicClock clock;
    private final Random random;
    private final MessageEncoder encoder;
    private final MessageDecoder decoder;
    private final MessageAuthenticator authenticator;
    @Getter
    private final DatagramHandler inbound = this::onDatagram;
    private final long electionTimeoutMinNanos;
    private final long electionTimeoutSpreadNanos;
    private final PrimosListener listener;

    /**
     * Where the counters go. Never null; {@link PrimosStatsListener#NONE} when nobody asked.
     */
    private final PrimosStatsListener stats;

    /**
     * When this node first wanted to lead, so a campaign can be timed end to end.
     *
     * <p>Set at the <em>pre-vote</em> rather than at the election, because the question being
     * answered is how long the failover took, and a pre-vote that had to be retried is part of it.
     */
    private long campaignStartedNanos = NO_CONTACT;

    /**
     * When this node last had a leader, so an outage can be timed across the elections inside it.
     */
    private long leaderlessSinceNanos = NO_CONTACT;
    private final Set<Integer> votesGranted = new HashSet<>();
    private final Set<Integer> preVotesGranted = new HashSet<>();
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
    private TrackedPeer[] peers;
    @Getter
    private Role role = Role.FOLLOWER;
    @Getter
    private long currentTerm;
    @Getter
    private int votedFor = PersistentState.NO_VOTE;
    private int leaderId = -1;
    @Getter
    private Leader leader;
    private long electionDeadlineNanos;
    private long heartbeatDeadlineNanos;
    private long lastLeaderContactNanos;
    private long probeDeadlineNanos;
    private long probeSequence;
    private long nodeConfigDeadlineNanos;
    private long roleChangedAtNanos;
    private boolean preVoting;
    private long preVoteTerm;
    private boolean started;
    /**
     * What killed the node, if anything has.
     */
    @Getter
    private Throwable failureCause;
    /**
     * How many pre-vote rounds this node has run, whether or not they led to an election.
     */
    @Getter
    private long preVotesStarted;
    @Getter
    private long electionsStarted;
    /**
     * How many times this node has stood itself down for lack of quorum contact.
     */
    @Getter
    private long leadershipsRelinquished;
    /**
     * How many times this node has campaigned because a leader handed over to it.
     */
    @Getter
    private long handoversAccepted;
    /**
     * How many peers this node has seen announce a clean departure.
     */
    @Getter
    private long departuresObserved;

    /**
     * Learned voters dropped for silence. Always zero on a node with a configured peer set.
     */
    @Getter
    private long peersForgotten;

    /**
     * The intervals a peer most recently stated, assembled and validated — witness only, null
     * until one arrives.
     */
    @Getter
    private Timings observedTimings;

    public ElectionNode(ClusterConfig config,
                        StateStore store,
                        Transport transport,
                        MonotonicClock clock,
                        Random random,
                        MessageAuthenticator authenticator) {
        this(config, store, transport, clock, random, authenticator, new PrimosListener() {
        }, PrimosStatsListener.NONE);
    }

    public ElectionNode(ClusterConfig config,
                        StateStore store,
                        Transport transport,
                        MonotonicClock clock,
                        Random random,
                        MessageAuthenticator authenticator,
                        PrimosListener listener) {
        this(config, store, transport, clock, random, authenticator, listener,
                PrimosStatsListener.NONE);
    }

    /**
     * @param listener a single sink for events. Phase 8's dispatcher implements this and fans out
     *                 to the application's listeners, so nothing here needs to know how many there
     *                 are or which thread they want.
     * @param stats    where the counters go, already guarded against throwing. Primitives only and
     *                 one sink, because some of these fire per datagram — see
     *                 {@link PrimosStatsListener}.
     */
    public ElectionNode(ClusterConfig config,
                        StateStore store,
                        Transport transport,
                        MonotonicClock clock,
                        Random random,
                        MessageAuthenticator authenticator,
                        PrimosListener listener,
                        PrimosStatsListener stats) {
        this.config = Objects.requireNonNull(config, "config");
        this.timings = config.timings();
        this.quorum = Quorum.of(config);
        this.store = Objects.requireNonNull(store, "store");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.electionTimeoutMinNanos = timings.electionTimeoutMin().toNanos();
        this.electionTimeoutSpreadNanos = timings.electionTimeoutSpread().toNanos();
        List<Peer> configured = config.allPeers();
        this.peers = new TrackedPeer[configured.size()];
        for (int i = 0; i < peers.length; i++) {
            peers[i] = new TrackedPeer(configured.get(i));
        }
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
        this.encoder = new MessageEncoder(config.applicationName(), config.nodeId(), authenticator);
        // A node answers only its configured peers, by id. A witness server cannot: its members
        // are not configured, so it has nothing to compare an id against and the authentication
        // tag is the whole of its admission control. See ClusterConfig.ClusterConfigBuilder#learnedMembership.
        this.decoder = new MessageDecoder(config.applicationName(), authenticator,
                config.isLearnedMembership() ? id -> true : this::isConfiguredPeer, this.stats);
    }

    private static boolean isSilentTooLong(TrackedPeer trackedPeer, long now, long silence) {
        return trackedPeer.lastHeardNanos >= 0 && now - trackedPeer.lastHeardNanos > silence;
    }

    private TrackedPeer trackedPeer(int id) {
        for (TrackedPeer peer : peers) {
            if (peer.peer.id() == id) {
                return peer;
            }
        }
        return null;
    }

    private boolean isConfiguredPeer(int id) {
        return trackedPeer(id) != null;
    }

    /**
     * Adds a voter that was not configured, for a witness server whose members arrive.
     *
     * @return whether this peer was new
     */
    public boolean learnPeer(Peer peer) {
        Objects.requireNonNull(peer, "peer");
        if (!config.isLearnedMembership()) {
            throw new IllegalStateException(
                    "node " + config.nodeId() + " has a configured peer set, so peer " + peer.id()
                            + " cannot be learned; build the config with learnedMembership()");
        }
        if (peer.id() == config.nodeId() || isConfiguredPeer(peer.id())) {
            return false;
        }
        TrackedPeer tracked = new TrackedPeer(peer);
        // Learned because a datagram arrived from it, so it has been heard from as of now. Without
        // this a new peer would look infinitely silent and be forgotten on the next probe round.
        tracked.lastHeardNanos = clock.nanoTime();

        int evicted = peers.length < MAX_LEARNED_PEERS ? -1 : leastRecentlyHeard();
        if (evicted < 0) {
            peers = Arrays.copyOf(peers, peers.length + 1);
            peers[peers.length - 1] = tracked;
        } else {
            // At the bound, so the quietest peer makes room. The bound is what stops a set keyed on
            // something arriving from the network being a leak somebody else controls; eviction is
            // cheap for the same reason forgetting is, since the next datagram teaches it again.
            Peer gone = peers[evicted].peer;
            log.info("cluster '{}': witness {} is tracking {} voters, so voter {} makes room for voter {}",
                    config.applicationName(), config.nodeId(), MAX_LEARNED_PEERS, gone.id(), peer.id());
            transport.forgetSender(gone.id());
            peers[evicted] = tracked;
        }
        log.info("cluster '{}': witness {} has learned voter {} at {}:{}",
                config.applicationName(), config.nodeId(), peer.id(), peer.host(), peer.port());
        return true;
    }

    private int leastRecentlyHeard() {
        int quietest = 0;
        for (int i = 1; i < peers.length; i++) {
            if (peers[i].lastHeardNanos < peers[quietest].lastHeardNanos) {
                quietest = i;
            }
        }
        return quietest;
    }

    public void start() {
        if (started) {
            throw new IllegalStateException("node " + config.nodeId() + " is already started");
        }
        started = true;

        PersistentState state = store.load();
        currentTerm = state.currentTerm();
        votedFor = state.votedFor();
        role = Role.FOLLOWER;
        roleChangedAtNanos = clock.nanoTime();

        if (config.isLearnedMembership()) {
            // There is no quorum summary to print: this witness does not know how many voters the
            // cluster has, and deliberately does not need to. It never campaigns and never leads,
            // so it never counts votes — it only answers them.
            log.info("cluster '{}': witness {} is serving, with the voter set learned from traffic",
                    config.applicationName(), config.nodeId());
        } else {
            log.info("{}", quorum.summary());
            for (String warning : quorum.warnings()) {
                log.warn("{}", warning);
            }
        }
        if (authenticator.isEnabled()) {
            // Named at startup because nothing on the wire carries it: a node configured with the
            // wrong algorithm is silent in exactly the way a node with the wrong secret is, and
            // this line is what tells the two apart from a log rather than a packet capture.
            log.info("cluster '{}': node {} authenticates datagrams with {}",
                    config.applicationName(), config.nodeId(), authenticator.algorithm());
        }
        if (!store.isDurable()) {
            log.warn("cluster '{}': node {} is using a state store that does not survive a crash."
                            + " After any restart this node may vote twice in one term, which can"
                            + " elect two leaders. Use this only for tests.",
                    config.applicationName(), config.nodeId());
        }
        lastLeaderContactNanos = clock.nanoTime();
        probeDeadlineNanos = clock.nanoTime() + timings.probeInterval().toNanos();
        // One probe interval, not immediately. A node starting up has peers that may not be
        // listening yet, so an advert sent on the first turn is likely to be thrown away and the
        // next one is half a minute off — while a second later they are up and a mismatch is
        // reported almost as promptly. Not zero for a second reason: a deadline of "now" is shared
        // by every node in the cluster, which would make nextDeadlineNanos() identical everywhere
        // at startup and hide the randomised election timeout behind it.
        nodeConfigDeadlineNanos = clock.nanoTime() + timings.probeInterval().toNanos();
        resetElectionTimer();
    }

    /**
     * One turn of the loop: wait for at most as long as the next deadline allows, deliver whatever
     * arrived, then fire whatever is due.
     *
     * @param maxWaitNanos an upper bound on the wait, so a caller can stay responsive to its own
     *                     concerns. The simulation passes zero, because there the harness owns time.
     */
    public void runOnce(long maxWaitNanos) {
        requireStarted();
        if (role == Role.FAILED) {
            return;
        }
        try {
            long wait = Math.min(maxWaitNanos, Math.max(0L, nextDeadlineNanos() - clock.nanoTime()));
            transport.poll(wait, inbound);
            fireDueTimers();
        } catch (Throwable failure) {
            fail(failure);
            if (failure instanceof Error) {
                throw (Error) failure;
            }
        }
    }

    private void fail(Throwable cause) {
        Role previous = role;
        setRole(Role.FAILED);
        setLeader(-1);
        failureCause = cause;
        abandonPreVote();
        votesGranted.clear();
        forgetQuorumContact();

        log.error("cluster '{}': node {} has failed while {} in term {} and will not lead again."
                        + " Restart the process; this node cannot recover in place.",
                config.applicationName(), config.nodeId(), previous, currentTerm, cause);
    }

    /**
     * Whether this node has failed permanently.
     */
    public boolean isFailed() {
        return role == Role.FAILED;
    }

    /**
     * When this node next has something to do.
     *
     * <p>A witness returns {@link Long#MAX_VALUE}: it never campaigns, so it has no election timer
     * and nothing to do until someone talks to it.
     */
    public long nextDeadlineNanos() {
        if (role == Role.FAILED) {
            return Long.MAX_VALUE;
        }
        long deadline = Math.min(probeDeadlineNanos, nodeConfigDeadlineNanos);
        if (role == Role.LEADER) {
            deadline = Math.min(deadline, heartbeatDeadlineNanos);
        } else if (!config.isWitness()) {
            deadline = Math.min(deadline, electionDeadlineNanos);
        }
        return deadline;
    }

    /**
     * Who is currently believed to be leading, if anyone.
     */
    public Optional<Leader> leaderOptional() {
        return Optional.ofNullable(leader);
    }

    private void setRole(Role next) {
        if (role == next) {
            return;
        }
        Role previous = role;
        role = next;
        roleChangedAtNanos = clock.nanoTime();
        listener.onRoleChanged(previous, next, currentTerm);
    }

    private void setLeader(int next) {
        if (leaderId == next) {
            return;
        }
        long now = clock.nanoTime();
        if (next < 0) {
            // Only the first loss starts the clock: an outage that runs through several elections
            // is one outage, and restarting it per election would report the last one instead.
            if (leaderlessSinceNanos == NO_CONTACT) {
                leaderlessSinceNanos = now;
            }
        } else if (leaderlessSinceNanos != NO_CONTACT) {
            stats.onLeaderKnown(currentTerm, now - leaderlessSinceNanos);
            leaderlessSinceNanos = NO_CONTACT;
        }
        leaderId = next;
        leader = identify(next);
        listener.onLeaderChanged(leaderOptional(), currentTerm);
    }

    private Leader identify(int id) {
        if (id < 0) {
            return null;
        }
        if (id == config.nodeId()) {
            return Leader.self(id);
        }
        TrackedPeer trackedPeer = trackedPeer(id);
        return trackedPeer == null ? Leader.byId(id) : Leader.at(trackedPeer.peer);
    }

    /**
     * Which voters this node is currently tracking.
     *
     * <p>Membership, not statistics — which is why it outlived {@code peerStats()}. It matters on a
     * witness, whose voter set arrives from traffic and is aged out again, so "who is being probed"
     * is a real question with a changing answer. On a configured node it is the peer set it was
     * built with.
     */
    public Set<Integer> trackedPeerIds() {
        Set<Integer> ids = new LinkedHashSet<>();
        for (TrackedPeer peer : peers) {
            ids.add(peer.peer.id());
        }
        return ids;
    }

    private void onDatagram(ByteBuffer datagram) {
        if (decoder.decode(datagram, this)) {
            // Only now, with the tag verified and the sender known, may the transport learn where
            // that peer is speaking from.
            int senderId = decoder.lastSenderId();
            transport.confirmSender(senderId);
            // Any authenticated datagram is being heard from, not only an answer to a probe. A
            // node that is voting and heartbeating is plainly present, and counting only probe
            // replies would let asymmetric loss make a busy peer look silent.
            TrackedPeer heard = trackedPeer(senderId);
            if (heard != null) {
                heard.lastHeardNanos = clock.nanoTime();
            }
        }
    }

    private void fireDueTimers() {
        long now = clock.nanoTime();
        if (now >= probeDeadlineNanos) {
            probeRound(now);
            probeDeadlineNanos = now + timings.probeInterval().toNanos();
        }
        if (now >= nodeConfigDeadlineNanos) {
            advertiseNodeConfig();
            nodeConfigDeadlineNanos = now + nextNodeConfigIntervalNanos();
        }
        if (role == Role.LEADER) {
            if (!hasQuorumContact(now)) {
                relinquishLeadership();
                return;
            }
            if (now >= heartbeatDeadlineNanos) {
                broadcastHeartbeat();
                heartbeatDeadlineNanos = now + timings.heartbeatInterval().toNanos();
            }
            return;
        }
        if (!config.isWitness() && now >= electionDeadlineNanos) {
            startPreVote();
        }
    }

    private void startPreVote() {
        preVoting = true;
        preVoteTerm = currentTerm + 1;
        preVotesGranted.clear();
        preVotesGranted.add(config.nodeId());
        preVotesStarted++;
        if (campaignStartedNanos == NO_CONTACT) {
            // Not reset per attempt: a failover that took three pre-vote rounds took as long as it
            // took, and timing only the last round would report the fastest part of the outage.
            campaignStartedNanos = clock.nanoTime();
        }
        resetElectionTimer();

        log.debug("node {} is asking whether term {} would elect it", config.nodeId(), preVoteTerm);

        if (quorum.isReached(preVotesGranted.size())) {
            startElection();
            return;
        }
        for (TrackedPeer trackedPeer : peers) {
            send(trackedPeer.peer.id(), encoder.requestVote(sendBuffer, preVoteTerm, true));
        }
    }

    private boolean wouldGrantVote(long proposedTerm, long now) {
        if (role == Role.LEADER) {
            return false;
        }
        if (proposedTerm < currentTerm) {
            return false;
        }
        return now - lastLeaderContactNanos >= timings.electionTimeoutMin().toNanos();
    }

    private void abandonPreVote() {
        preVoting = false;
        preVotesGranted.clear();
    }

    /**
     * Closes a campaign, won or lost, and reports how long it took.
     *
     * <p>A lost one is the more interesting of the two when a cluster will not fail over: a
     * pre-vote refused is <em>why no election happened</em>, and no other signal says so.
     *
     * <p>Deliberately <strong>not</strong> hooked into {@link #abandonPreVote()}, which reads as
     * the place a campaign ends and is not: {@link #startElection()} calls it on the way from a
     * <em>won</em> pre-vote into the real election, so ending the campaign there would report every
     * successful failover as a lost one. A campaign ends when this node leads, or when it steps
     * down without leading.
     */
    private void endCampaign(boolean won) {
        if (campaignStartedNanos == NO_CONTACT) {
            return;
        }
        long took = clock.nanoTime() - campaignStartedNanos;
        campaignStartedNanos = NO_CONTACT;
        if (won) {
            stats.onElectionWon(currentTerm, took);
        } else {
            stats.onElectionLost(currentTerm, took);
        }
    }

    private void startElection() {
        abandonPreVote();
        currentTerm++;
        votedFor = config.nodeId();
        persist();

        setRole(Role.CANDIDATE);
        setLeader(-1);
        votesGranted.clear();
        votesGranted.add(config.nodeId());
        electionsStarted++;
        resetElectionTimer();

        logVote("cluster '{}': node {} is standing for election in term {} and voted for itself, needing {} of {} votes",
                config.applicationName(), config.nodeId(), currentTerm, quorum.required(), quorum.voters());

        if (quorum.isReached(votesGranted.size())) {
            becomeLeader();
            return;
        }
        for (TrackedPeer trackedPeer : peers) {
            send(trackedPeer.peer.id(), encoder.requestVote(sendBuffer, currentTerm, false));
        }
    }

    private void becomeLeader() {
        abandonPreVote();
        setRole(Role.LEADER);
        setLeader(config.nodeId());
        endCampaign(true);

        // Start the contact clock now, or a new leader would step down before its first heartbeat
        // had a chance to be answered.
        long now = clock.nanoTime();
        for (TrackedPeer trackedPeer : peers) {
            trackedPeer.lastRoundTripNanos = now;
        }

        log.info("cluster '{}': node {} became leader for term {}",
                config.applicationName(), config.nodeId(), currentTerm);

        broadcastHeartbeat();
        heartbeatDeadlineNanos = clock.nanoTime() + timings.heartbeatInterval().toNanos();
    }

    private void stepDown(long term) {
        abandonPreVote();
        // Campaigning and no longer campaigning, without having won.
        endCampaign(false);
        currentTerm = term;
        votedFor = PersistentState.NO_VOTE;
        persist();

        Role previous = role;
        setRole(Role.FOLLOWER);
        setLeader(-1);
        votesGranted.clear();
        resetElectionTimer();

        if (previous != Role.FOLLOWER) {
            log.info("cluster '{}': node {} stepped down from {} on seeing term {}",
                    config.applicationName(), config.nodeId(), previous, term);
        }
    }

    private void broadcastHeartbeat() {
        for (TrackedPeer peer : peers) {
            send(peer.peer.id(), encoder.appendEntries(sendBuffer, currentTerm, 0L));
        }
    }

    /**
     * How long until this node restates itself: probe cadence while anybody is silent, else the
     * advertisement interval.
     */
    private long nextNodeConfigIntervalNanos() {
        for (TrackedPeer peer : peers) {
            if (!peer.reachable) {
                return timings.probeInterval().toNanos();
            }
        }
        return timings.advertisementInterval().toNanos();
    }

    private void advertiseNodeConfig() {
        for (TrackedPeer peer : peers) {
            send(peer.peer.id(), encoder.nodeConfig(sendBuffer, currentTerm,
                    (int) timings.heartbeatInterval().toMillis(),
                    (int) timings.electionTimeoutMin().toMillis(),
                    (int) timings.electionTimeoutMax().toMillis(),
                    (int) timings.probeInterval().toMillis(),
                    timings.failureThreshold()));
        }
    }

    private void logVote(String message, Object... args) {
        if (config.voteLogging()) {
            log.info(message, args);
        } else {
            log.debug(message, args);
        }
    }

    @Override
    public void onRequestVote(int senderId, long term, boolean preVote) {
        if (preVote) {
            // Deliberately no step-down, no persistence and no timer reset. A pre-vote must be
            // answerable without changing anything, or it would inflate terms exactly as the real
            // vote it is meant to avoid, and a node could suppress elections by spamming them.
            boolean would = wouldGrantVote(term, clock.nanoTime());
            // Worth a line even though nothing changed: a pre-vote that is refused is why an
            // election never happens, and without this the outage it explains is silent.
            logVote(would
                            ? "cluster '{}': node {} would vote for node {} in a term {} election"
                            : "cluster '{}': node {} would not vote for node {} in a term {}"
                            + " election; it has heard from a leader too recently, or its own"
                            + " term is ahead",
                    config.applicationName(), config.nodeId(), senderId, term);
            send(senderId, encoder.requestVoteReply(sendBuffer, term, would, true));
            return;
        }
        if (term > currentTerm) {
            stepDown(term);
        }
        boolean granted = term == currentTerm
                && (votedFor == PersistentState.NO_VOTE || votedFor == senderId);

        if (granted && votedFor != senderId) {
            votedFor = senderId;
            persist();
            resetElectionTimer();
        }
        if (granted) {
            logVote("cluster '{}': node {} granted its vote in term {} to node {}",
                    config.applicationName(), config.nodeId(), currentTerm, senderId);
        } else if (term < currentTerm) {
            logVote("cluster '{}': node {} refused node {} a vote for term {}; it is already in term {}",
                    config.applicationName(), config.nodeId(), senderId, term, currentTerm);
        } else {
            logVote("cluster '{}': node {} refused node {} a vote in term {}; it has already voted for {} in that term",
                    config.applicationName(), config.nodeId(), senderId, currentTerm,
                    votedFor == config.nodeId() ? "itself" : "node " + votedFor);
        }
        send(senderId, encoder.requestVoteReply(sendBuffer, currentTerm, granted, preVote));
    }

    @Override
    public void onRequestVoteReply(int senderId, long term, boolean granted, boolean preVote) {
        if (preVote) {
            // The reply echoes the term that was proposed, not the responder's own, so a reply
            // from an abandoned round cannot be counted in this one.
            if (!preVoting || term != preVoteTerm || !granted) {
                return;
            }
            if (preVotesGranted.add(senderId)) {
                logVote("cluster '{}': node {} has {} of the {} pre-votes it needs for term {}, after node {} agreed",
                        config.applicationName(), config.nodeId(), preVotesGranted.size(), quorum.required(), term, senderId);
                if (quorum.isReached(preVotesGranted.size())) {
                    startElection();
                }
            }
            return;
        }
        if (term > currentTerm) {
            stepDown(term);
            return;
        }
        if (role != Role.CANDIDATE || term != currentTerm) {
            // A reply from a term we have left. Common on UDP, and never interesting.
            return;
        }
        if (!granted) {
            return;
        }
        // A set, not a counter: a duplicated datagram must not be a second vote.
        if (votesGranted.add(senderId)) {
            logVote("cluster '{}': node {} has {} of the {} votes it needs in term {}, after node {} voted for it",
                    config.applicationName(), config.nodeId(), votesGranted.size(), quorum.required(), currentTerm, senderId);
            if (quorum.isReached(votesGranted.size())) {
                becomeLeader();
            }
        }
    }

    @Override
    public void onNodeConfig(int senderId, long term, int heartbeatMillis, int electionMinMillis,
                             int electionMaxMillis, int probeMillis, int failureThreshold) {
        if (term > currentTerm) {
            stepDown(term);
        }
        // Before the peer lookup, deliberately. A witness learns its voters from traffic, so a
        // node that has just stated its configuration is not in the table until this datagram has
        // finished being handled — and under ConfigSource.CLUSTER that first statement is the one
        // that created the application, so requiring a known sender would discard precisely the
        // message the whole mode is built on. Same shape as the reply that went nowhere because
        // confirmSender had not run yet.
        if (config.isWitness()) {
            recordObservedTimings(senderId, heartbeatMillis, electionMinMillis, electionMaxMillis,
                    probeMillis, failureThreshold);
        }
        TrackedPeer trackedPeer = trackedPeer(senderId);
        if (trackedPeer == null) {
            return;
        }
        if (config.isFollowingStatedTimings()) {
            // Recorded above, and that is the whole of it. A difference here is not somebody's
            // failover being quietly slower — it is the value this node is about to be rebuilt
            // with, so the ERROR would fire once on every retune and be resolved a turn later.
            return;
        }
        boolean matches = heartbeatMillis == (int) timings.heartbeatInterval().toMillis()
                && electionMinMillis == (int) timings.electionTimeoutMin().toMillis()
                && electionMaxMillis == (int) timings.electionTimeoutMax().toMillis()
                && probeMillis == (int) timings.probeInterval().toMillis()
                && failureThreshold == timings.failureThreshold();
        if (matches == trackedPeer.timingsMatch) {
            return;
        }
        trackedPeer.timingsMatch = matches;
        if (matches) {
            log.info("cluster '{}': node {} now runs the same intervals as node {}",
                    config.applicationName(), senderId, config.nodeId());
            return;
        }
        stats.onTimingMismatch(senderId);
        log.error("cluster '{}': node {} runs different intervals from node {} —"
                        + " heartbeat {}ms vs {}ms, election {}-{}ms vs {}-{}ms,"
                        + " probe {}ms vs {}ms, failure threshold {} vs {}."
                        + " Nothing is unsafe and the cluster keeps working, but failover is"
                        + " slower than it looks: a node with a longer election timeout is"
                        + " proportionally more reluctant to grant the pre-vote a failover waits"
                        + " on. Make the profile uniform across every member, witness included.",
                config.applicationName(), senderId, config.nodeId(),
                heartbeatMillis, timings.heartbeatInterval().toMillis(),
                electionMinMillis, electionMaxMillis,
                timings.electionTimeoutMin().toMillis(), timings.electionTimeoutMax().toMillis(),
                probeMillis, timings.probeInterval().toMillis(),
                failureThreshold, timings.failureThreshold());
    }

    /**
     * Assembles what a peer stated into a {@link Timings}, so an owner can read one value.
     */
    private void recordObservedTimings(int senderId, int heartbeatMillis, int electionMinMillis,
                                       int electionMaxMillis, int probeMillis,
                                       int failureThreshold) {
        try {
            Duration probe = Duration.ofMillis(probeMillis);
            Duration advertisement = timings.advertisementInterval().compareTo(probe) < 0
                    ? probe
                    : timings.advertisementInterval();
            Timings stated = Timings.builder()
                    .heartbeatInterval(Duration.ofMillis(heartbeatMillis))
                    .electionTimeoutMin(Duration.ofMillis(electionMinMillis))
                    .electionTimeoutMax(Duration.ofMillis(electionMaxMillis))
                    .probeInterval(probe)
                    .advertisementInterval(advertisement)
                    .failureThreshold(failureThreshold)
                    .build();
            observedTimings = stated;
        } catch (RuntimeException e) {
            stats.onUnusableNodeConfig(senderId);
            log.warn("cluster '{}': node {} stated intervals that do not hold together and were ignored: {}",
                    config.applicationName(), senderId, e.getMessage());
        }
    }

    @Override
    public void onAppendEntries(int senderId, long term, long leaderVersion) {
        if (term < currentTerm) {
            send(senderId, encoder.appendEntriesReply(sendBuffer, currentTerm, false));
            return;
        }
        if (term > currentTerm) {
            stepDown(term);
        }
        // A leader exists in this term, so stop campaigning and stop counting down to an election.
        abandonPreVote();
        setRole(Role.FOLLOWER);
        setLeader(senderId);
        votesGranted.clear();
        lastLeaderContactNanos = clock.nanoTime();
        resetElectionTimer();

        send(senderId, encoder.appendEntriesReply(sendBuffer, currentTerm, true));
    }

    @Override
    public void onAppendEntriesReply(int senderId, long term, boolean success) {
        if (term > currentTerm) {
            stepDown(term);
            return;
        }
        // This is the only thing that proves a round trip: the peer received a heartbeat this node
        // sent, and answered it. See hasQuorumContact.
        if (role == Role.LEADER && term == currentTerm) {
            TrackedPeer trackedPeer = trackedPeer(senderId);
            if (trackedPeer != null) {
                trackedPeer.lastRoundTripNanos = clock.nanoTime();
            }
        }
    }

    @Override
    public void onPing(int senderId, long term, long sequence) {
        if (term > currentTerm) {
            stepDown(term);
        }
        send(senderId, encoder.pingReply(sendBuffer, currentTerm, sequence));
    }

    /**
     * A departing leader has asked this node to campaign now. Raft thesis §3.10.
     */
    @Override
    public void onTimeoutNow(int senderId, long term) {
        if (config.isWitness()) {
            // The one hard rule about a witness: it never campaigns, so it never leads. A leader
            // should never target one, and this is the second half of that guarantee — the half
            // that does not depend on the sender being a version that knows the rule.
            log.debug("cluster '{}': witness {} ignoring a handover from node {}",
                    config.applicationName(), config.nodeId(), senderId);
            return;
        }
        if (term < currentTerm) {
            // From a leader this node has already left behind. Acting would campaign on behalf of
            // a term that is over.
            return;
        }
        if (term > currentTerm) {
            stepDown(term);
        }
        if (role == Role.LEADER) {
            return;
        }
        handoversAccepted++;
        stats.onHandoverAccepted(currentTerm);
        log.info("cluster '{}': node {} is campaigning immediately, handed over by node {} in"
                + " term {}", config.applicationName(), config.nodeId(), senderId, term);
        startElection();
    }

    /**
     * A peer has said it is shutting down, so it is marked unreachable now rather than in silence.
     */
    @Override
    public void onLeaving(int senderId, long term) {
        if (term > currentTerm) {
            // The universal rule, applied here as everywhere else. Nothing below depends on it.
            stepDown(term);
        }
        TrackedPeer trackedPeer = trackedPeer(senderId);
        if (trackedPeer == null) {
            return;
        }
        trackedPeer.awaitingReply = false;
        // Held at the threshold rather than merely marked down, so it stays down until the peer
        // actually answers something. A reply resets this to zero and reports it up again.
        trackedPeer.consecutiveMisses = timings.failureThreshold();
        departuresObserved++;
        stats.onDepartureObserved(senderId);
        if (!trackedPeer.reachable) {
            // Already down, or never yet seen up. Nothing changed, so nothing is reported — the
            // same discipline the probe path follows, so one departure is never two events.
            return;
        }
        trackedPeer.reachable = false;
        log.info("cluster '{}': node {} {} {} said it is shutting down",
                config.applicationName(), config.nodeId(), trackedPeer.peer.isWitness() ? "witness" : "peer", senderId);
        listener.onPeerDown(trackedPeer.peer);
    }

    /**
     * Tells every peer this node is going away, so none of them has to wait out a probe timeout.
     */
    public int announceDeparture() {
        if (peers.length == 0) {
            return 0;
        }
        for (TrackedPeer peer : peers) {
            send(peer.peer.id(), encoder.leaving(sendBuffer, currentTerm));
        }
        log.info("cluster '{}': node {} told {} peer(s) it is shutting down",
                config.applicationName(), config.nodeId(), peers.length);
        return peers.length;
    }

    /**
     * Hands leadership to a follower on the way out, instead of just going quiet.
     */
    public boolean handOverLeadership() {
        if (role != Role.LEADER) {
            return false;
        }
        Peer target = handoverTarget();
        if (target == null) {
            log.info("cluster '{}': node {} is stopping as leader of term {} with no reachable"
                            + " follower to hand over to; the cluster will elect a replacement on"
                            + " its own timeout", config.applicationName(), config.nodeId(),
                    currentTerm);
            return false;
        }
        stats.onHandoverSent(target.id());
        log.info("cluster '{}': node {} is stopping and handing leadership of term {} to node {}",
                config.applicationName(), config.nodeId(), currentTerm, target.id());
        send(target.id(), encoder.timeoutNow(sendBuffer, currentTerm));
        return true;
    }

    private Peer handoverTarget() {
        long now = clock.nanoTime();
        long window = timings.electionTimeoutMin().toNanos();
        Peer best = null;
        long bestContactNanos = Long.MIN_VALUE;

        for (TrackedPeer trackedPeer : peers) {
            if (trackedPeer.peer.isWitness() || trackedPeer.consecutiveMisses >= timings.failureThreshold()) {
                continue;
            }
            long lastContact = trackedPeer.lastRoundTripNanos;
            if (lastContact == NO_CONTACT || now - lastContact > window) {
                continue;
            }
            if (best == null || lastContact > bestContactNanos) {
                best = trackedPeer.peer;
                bestContactNanos = lastContact;
            }
        }
        return best;
    }

    @Override
    public void onPingReply(int senderId, long term, long sequence) {
        if (term > currentTerm) {
            stepDown(term);
        }
        TrackedPeer trackedPeer = trackedPeer(senderId);
        if (trackedPeer == null || sequence != trackedPeer.outstandingSequence) {
            // A reply to a probe we have already given up on. Counting it would let one badly
            // delayed datagram keep a peer that is long gone looking alive.
            return;
        }
        long now = clock.nanoTime();
        trackedPeer.awaitingReply = false;
        trackedPeer.consecutiveMisses = 0;
        trackedPeer.lastProbeRoundTripNanos = now - trackedPeer.sentAtNanos;
        stats.onProbeRoundTrip(senderId, trackedPeer.lastProbeRoundTripNanos);
        trackedPeer.lastHeardNanos = now;
        if (!trackedPeer.reachable) {
            trackedPeer.reachable = true;
            notifyPeerUp(trackedPeer.peer);
        }
    }

    private void probeRound(long now) {
        forgetSilentPeers(now);
        for (TrackedPeer trackedPeer : peers) {
            if (trackedPeer.awaitingReply) {
                trackedPeer.consecutiveMisses++;
                if (trackedPeer.reachable && trackedPeer.consecutiveMisses >= timings.failureThreshold()) {
                    trackedPeer.reachable = false;
                    notifyPeerDown(trackedPeer.peer);
                }
            }
            trackedPeer.outstandingSequence = ++probeSequence;
            trackedPeer.sentAtNanos = now;
            trackedPeer.awaitingReply = true;
            send(trackedPeer.peer.id(), encoder.ping(sendBuffer, currentTerm, trackedPeer.outstandingSequence));
        }
    }

    /**
     * Drops learned voters that have gone quiet for far longer than being down.
     */
    private void forgetSilentPeers(long now) {
        if (!config.isLearnedMembership()) {
            return;
        }
        long silence = timings.probeInterval().toNanos() * FORGET_AFTER_PROBES;
        int surviving = 0;
        for (TrackedPeer trackedPeer : peers) {
            if (!isSilentTooLong(trackedPeer, now, silence)) {
                surviving++;
            }
        }
        if (surviving == peers.length) {
            // The steady state, and it allocates nothing: a scan and no array.
            return;
        }
        TrackedPeer[] kept = new TrackedPeer[surviving];
        int at = 0;
        for (TrackedPeer trackedPeer : peers) {
            if (isSilentTooLong(trackedPeer, now, silence)) {
                log.info("cluster '{}': witness {} has not heard from voter {} for {}s and has"
                                + " stopped tracking it; it will be learned again if it returns",
                        config.applicationName(), config.nodeId(), trackedPeer.peer.id(),
                        TimeUnit.NANOSECONDS.toSeconds(now - trackedPeer.lastHeardNanos));
                transport.forgetSender(trackedPeer.peer.id());
                peersForgotten++;
                stats.onPeerForgotten(trackedPeer.peer.id());
            } else {
                kept[at++] = trackedPeer;
            }
        }
        peers = kept;
    }

    private void notifyPeerUp(Peer peer) {
        log.info("cluster '{}': node {} {} {} is reachable", config.applicationName(), config.nodeId(), peer.isWitness() ? "witness" : "peer", peer.id());
        listener.onPeerUp(peer);
    }

    private void notifyPeerDown(Peer peer) {
        if (peer.isWitness()) {
            log.warn("cluster '{}': the witness ({}) is unreachable. Nothing looks broken, but this cluster can no longer survive a partition.",
                    config.applicationName(), peer.id());
        } else {
            log.warn("cluster '{}': peer {} is unreachable", config.applicationName(), peer.id());
        }
        listener.onPeerDown(peer);
    }

    /**
     * Whether a peer is currently answering probes.
     */
    public boolean isReachable(int peerId) {
        TrackedPeer trackedPeer = trackedPeer(peerId);
        return trackedPeer != null && trackedPeer.reachable;
    }

    private boolean hasQuorumContact(long now) {
        long window = timings.electionTimeoutMin().toNanos();
        int inContact = 1;
        for (TrackedPeer trackedPeer : peers) {
            long lastHeard = trackedPeer.lastRoundTripNanos;
            if (lastHeard != NO_CONTACT && now - lastHeard <= window) {
                inContact++;
            }
        }
        return quorum.isReached(inContact);
    }

    private void relinquishLeadership() {
        leadershipsRelinquished++;
        stats.onLeadershipRelinquished(currentTerm);
        log.warn("cluster '{}': node {} stood down as leader of term {}: no round trip with a"
                        + " majority within {}ms, so it may be partitioned from the cluster",
                config.applicationName(), config.nodeId(), currentTerm,
                timings.electionTimeoutMin().toMillis());

        setRole(Role.FOLLOWER);
        setLeader(-1);
        votesGranted.clear();
        forgetQuorumContact();
        resetElectionTimer();
    }

    private void forgetQuorumContact() {
        for (TrackedPeer trackedPeer : peers) {
            trackedPeer.lastRoundTripNanos = NO_CONTACT;
        }
    }

    private void resetElectionTimer() {
        electionDeadlineNanos = clock.nanoTime() + electionTimeoutMinNanos + (long) (random.nextDouble() * electionTimeoutSpreadNanos);
    }

    private void persist() {
        store.save(currentTerm, votedFor);
    }

    private void send(int peerId, ByteBuffer datagram) {
        try {
            transport.send(peerId, datagram);
        } catch (RuntimeException e) {
            log.debug("node {} could not send to {}: {}", config.nodeId(), peerId, e.toString());
        }
    }

    /**
     * Refuses a turn of the loop taken before the store has been read.
     */
    private void requireStarted() {
        if (!started) {
            throw new IllegalStateException("node " + config.nodeId() + " has not been started");
        }
    }

    @Override
    public String toString() {
        return "node " + config.nodeId() + " " + role + "/" + currentTerm;
    }

    private static final class TrackedPeer {

        private final Peer peer;

        private long outstandingSequence = -1;
        private long sentAtNanos;
        private boolean awaitingReply;
        private int consecutiveMisses;
        private boolean reachable;
        private long lastProbeRoundTripNanos = -1;
        private long lastHeardNanos = -1;
        private boolean timingsMatch = true;
        private long lastRoundTripNanos = NO_CONTACT;

        private TrackedPeer(Peer peer) {
            this.peer = peer;
        }
    }
}