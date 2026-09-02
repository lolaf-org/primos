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
     *
     * <p>Derived rather than configured, so it scales with whatever tolerance a deployment already
     * chose: a minute on the LAN defaults, ten on the garbage-collection-tolerant ten-second probe
     * interval the departure tests use. Far above {@code failureThreshold}, because down and gone
     * are different questions and only the second one is being asked here.
     */
    private static final int FORGET_AFTER_PROBES = 60;
    /**
     * How many voters a witness will track for one application before the quietest makes room.
     *
     * <p>The bound rather than the tidiness. Ageing keeps the set to who has spoken recently, which
     * bounds it by legitimate traffic; this bounds it against a keyholder cycling sender ids faster
     * than that. Same rule as the remembered-name set: anything keyed on something arriving from
     * the network needs a ceiling.
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
    private final DropCounters drops = new DropCounters();
    private final DatagramHandler inbound = this::onDatagram;
    private final long electionTimeoutMinNanos;
    private final long electionTimeoutSpreadNanos;
    private final PrimosListener listener;
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
    @Getter
    private long electionsWon;
    /**
     * How many times this node has stood itself down for lack of quorum contact.
     */
    @Getter
    private long leadershipsRelinquished;
    /**
     * How many times this node has handed leadership over on the way out.
     */
    @Getter
    private long handoversSent;
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
    private long timingMismatches;

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

    private long unusableNodeConfigs;


    public ElectionNode(ClusterConfig config,
                        StateStore store,
                        Transport transport,
                        MonotonicClock clock,
                        Random random,
                        MessageAuthenticator authenticator) {
        this(config, store, transport, clock, random, authenticator, new PrimosListener() {
        });
    }

    /**
     * @param listener a single sink for events. Phase 8's dispatcher implements this and fans out
     *                 to the application's listeners, so nothing here needs to know how many there
     *                 are or which thread they want.
     */
    public ElectionNode(ClusterConfig config,
                        StateStore store,
                        Transport transport,
                        MonotonicClock clock,
                        Random random,
                        MessageAuthenticator authenticator,
                        PrimosListener listener) {
        this.config = Objects.requireNonNull(config, "config");
        this.timings = config.timings();
        this.quorum = Quorum.of(config);
        this.store = Objects.requireNonNull(store, "store");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.listener = Objects.requireNonNull(listener, "listener");
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
        // tag is the whole of its admission control. See ClusterConfig.Builder#learnedMembership.
        this.decoder = new MessageDecoder(config.applicationName(), authenticator,
                config.isLearnedMembership() ? id -> true : this::isConfiguredPeer, drops);
    }

    private static boolean isSilentTooLong(TrackedPeer probe, long now, long silence) {
        return probe.lastHeardNanos >= 0 && now - probe.lastHeardNanos > silence;
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
     * <p>Only legal with {@link ClusterConfig.Builder#learnedMembership()}, and only worth calling
     * once a datagram from that sender has passed authentication — the caller is trusted to have
     * checked, because by the time this is reached the decoder already has.
     *
     * <p>What it buys is the probe: a witness that only ever replied would never notice a node had
     * gone, and its half of the probe exchange is also what keeps a NAT pinhole open in the
     * inbound direction. Call it between turns of the loop rather than from inside one.
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
        TrackedPeer probe = trackedPeer(id);
        return probe == null ? Leader.byId(id) : Leader.at(probe.peer);
    }

    /**
     * An immutable snapshot of this node's counters, for {@link org.lolaf.primos.PrimosNode#stats()}.
     */
    public PrimosStats stats() {
        return new PrimosStats(role, currentTerm,
                Duration.ofNanos(clock.nanoTime() - roleChangedAtNanos),
                electionsStarted, electionsWon, handoversSent, handoversAccepted,
                departuresObserved, timingMismatches, drops.snapshot(), peerStats());
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
        resetElectionTimer();

        log.debug("node {} is asking whether term {} would elect it", config.nodeId(), preVoteTerm);

        if (quorum.isReached(preVotesGranted.size())) {
            startElection();
            return;
        }
        for (TrackedPeer peer : peers) {
            send(peer.peer.id(), encoder.requestVote(sendBuffer, preVoteTerm, true));
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
        for (TrackedPeer peer : peers) {
            send(peer.peer.id(), encoder.requestVote(sendBuffer, currentTerm, false));
        }
    }

    private void becomeLeader() {
        abandonPreVote();
        setRole(Role.LEADER);
        setLeader(config.nodeId());
        electionsWon++;

        // Start the contact clock now, or a new leader would step down before its first heartbeat
        // had a chance to be answered.
        long now = clock.nanoTime();
        for (TrackedPeer probe : peers) {
            probe.lastRoundTripNanos = now;
        }

        log.info("cluster '{}': node {} became leader for term {}",
                config.applicationName(), config.nodeId(), currentTerm);

        broadcastHeartbeat();
        heartbeatDeadlineNanos = clock.nanoTime() + timings.heartbeatInterval().toNanos();
    }

    private void stepDown(long term) {
        abandonPreVote();
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
            send(peer.peer.id(), encoder.appendEntries(sendBuffer, currentTerm));
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
        TrackedPeer probe = trackedPeer(senderId);
        if (probe == null) {
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
        if (matches == probe.timingsMatch) {
            return;
        }
        probe.timingsMatch = matches;
        if (matches) {
            log.info("cluster '{}': node {} now runs the same intervals as node {}",
                    config.applicationName(), senderId, config.nodeId());
            return;
        }
        timingMismatches++;
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
     *
     * <p>All five are taken together rather than the one that changes a witness's mind, because a
     * set built from a valid profile is internally consistent and a field spliced into another
     * profile need not be: a witness on WAN intervals taking a LAN cluster's 300ms
     * {@code electionTimeoutMin} beside its own 200ms heartbeat is below the five-heartbeat floor
     * and would not build at all.
     *
     * <p>{@code advertisementInterval} is not on the wire — it only sets how often a node restates
     * values that cannot change — so this node's own is kept, raised to the stated probe interval
     * where that is longer, since a profile may not advertise faster than it probes.
     *
     * <p>Validation is {@link Timings}' own, which is the whole of the guard and is enough. It
     * rejects a set whose relationships do not hold; a set whose relationships do hold can only be
     * as wrong as the cluster that is running it, and that cluster has already broken itself worse
     * than it broke anyone watching.
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
            unusableNodeConfigs++;
            log.warn("cluster '{}': node {} stated intervals that do not hold together and were"
                            + " ignored: {}",
                    config.applicationName(), senderId, e.getMessage());
        }
    }

    /**
     * Node configs discarded because the intervals they stated could not form a valid profile.
     */
    public long unusableNodeConfigs() {
        return unusableNodeConfigs;
    }

    @Override
    public void onAppendEntries(int senderId, long term) {
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
            TrackedPeer probe = trackedPeer(senderId);
            if (probe != null) {
                probe.lastRoundTripNanos = clock.nanoTime();
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
        TrackedPeer probe = trackedPeer(senderId);
        if (probe == null) {
            return;
        }
        probe.awaitingReply = false;
        // Held at the threshold rather than merely marked down, so it stays down until the peer
        // actually answers something. A reply resets this to zero and reports it up again.
        probe.consecutiveMisses = timings.failureThreshold();
        departuresObserved++;
        if (!probe.reachable) {
            // Already down, or never yet seen up. Nothing changed, so nothing is reported — the
            // same discipline the probe path follows, so one departure is never two events.
            return;
        }
        probe.reachable = false;
        log.info("cluster '{}': {} {} said it is shutting down",
                config.applicationName(), probe.peer.isWitness() ? "witness" : "peer", senderId);
        listener.onPeerDown(probe.peer);
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
        handoversSent++;
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

        for (TrackedPeer probe : peers) {
            if (probe.peer.isWitness() || probe.consecutiveMisses >= timings.failureThreshold()) {
                continue;
            }
            long lastContact = probe.lastRoundTripNanos;
            if (lastContact == NO_CONTACT || now - lastContact > window) {
                continue;
            }
            if (best == null || lastContact > bestContactNanos) {
                best = probe.peer;
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
        TrackedPeer probe = trackedPeer(senderId);
        if (probe == null || sequence != probe.outstandingSequence) {
            // A reply to a probe we have already given up on. Counting it would let one badly
            // delayed datagram keep a peer that is long gone looking alive.
            return;
        }
        long now = clock.nanoTime();
        probe.awaitingReply = false;
        probe.consecutiveMisses = 0;
        probe.lastProbeRoundTripNanos = now - probe.sentAtNanos;
        probe.lastHeardNanos = now;
        if (!probe.reachable) {
            probe.reachable = true;
            notifyPeerUp(probe.peer);
        }
    }

    private void probeRound(long now) {
        forgetSilentPeers(now);
        for (TrackedPeer probe : peers) {
            if (probe.awaitingReply) {
                probe.consecutiveMisses++;
                if (probe.reachable && probe.consecutiveMisses >= timings.failureThreshold()) {
                    probe.reachable = false;
                    notifyPeerDown(probe.peer);
                }
            }
            probe.outstandingSequence = ++probeSequence;
            probe.sentAtNanos = now;
            probe.awaitingReply = true;
            send(probe.peer.id(), encoder.ping(sendBuffer, currentTerm, probe.outstandingSequence));
        }
    }

    /**
     * Drops learned voters that have gone quiet for far longer than being down.
     *
     * <p>Without this a witness only ever grows: {@link #learnPeer} adds and nothing removed, so a
     * tenant whose node ids roll — ephemeral instances taking a new one each deploy — leaves a
     * tracked peer and a probe every interval behind for every id it has ever used, for the life
     * of the process.
     *
     * <p><strong>Only a witness does this, and it cannot cost anybody a leader.</strong> A node
     * with a configured peer set never reaches here, because its peers are its quorum. A witness
     * has no quorum to change: {@code Quorum.unknown} is never reached, it never campaigns and
     * never leads, so its peer set feeds probing, reporting and departure datagrams and no
     * decision that could elect anyone.
     *
     * <p><strong>Gone is not down.</strong> Down is {@code failureThreshold} missed probes, and a
     * peer that is down is exactly the one a witness should keep watching — a garbage collection
     * or a brief partition must not lose it. The window here is far longer, and being wrong about
     * it costs nothing anyway: the next authenticated datagram from that node learns it again,
     * which is how it arrived.
     *
     * <p>A departure announcement deliberately does not trigger this. {@code Leaving} is a hint
     * that can be wrong and measurement stays authoritative — and making eviction reachable by one
     * datagram is a door worth not opening, however harmless it would be.
     */
    private void forgetSilentPeers(long now) {
        if (!config.isLearnedMembership()) {
            return;
        }
        long silence = timings.probeInterval().toNanos() * FORGET_AFTER_PROBES;
        int surviving = 0;
        for (TrackedPeer probe : peers) {
            if (!isSilentTooLong(probe, now, silence)) {
                surviving++;
            }
        }
        if (surviving == peers.length) {
            // The steady state, and it allocates nothing: a scan and no array.
            return;
        }
        TrackedPeer[] kept = new TrackedPeer[surviving];
        int at = 0;
        for (TrackedPeer probe : peers) {
            if (isSilentTooLong(probe, now, silence)) {
                log.info("cluster '{}': witness {} has not heard from voter {} for {}s and has"
                                + " stopped tracking it; it will be learned again if it returns",
                        config.applicationName(), config.nodeId(), probe.peer.id(),
                        TimeUnit.NANOSECONDS.toSeconds(now - probe.lastHeardNanos));
                transport.forgetSender(probe.peer.id());
                peersForgotten++;
            } else {
                kept[at++] = probe;
            }
        }
        peers = kept;
    }

    private void notifyPeerUp(Peer peer) {
        log.info("cluster '{}': {} {} is reachable", config.applicationName(), peer.isWitness() ? "witness" : "peer", peer.id());
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
     * What this node has observed about each peer, for {@link org.lolaf.primos.PrimosStats}.
     */
    public Map<Integer, PeerStats> peerStats() {
        long now = clock.nanoTime();
        Map<Integer, PeerStats> snapshot = new LinkedHashMap<>();
        for (TrackedPeer probe : peers) {
            snapshot.put(probe.peer.id(), new PeerStats(
                    probe.peer.id(),
                    probe.peer.type(),
                    probe.reachable,
                    probe.lastProbeRoundTripNanos < 0
                            ? null
                            : Duration.ofNanos(probe.lastProbeRoundTripNanos),
                    probe.lastHeardNanos < 0 ? null : Duration.ofNanos(now - probe.lastHeardNanos)));
        }
        return snapshot;
    }

    /**
     * Whether a peer is currently answering probes.
     */
    public boolean isReachable(int peerId) {
        TrackedPeer probe = trackedPeer(peerId);
        return probe != null && probe.reachable;
    }

    private boolean hasQuorumContact(long now) {
        long window = timings.electionTimeoutMin().toNanos();
        int inContact = 1;
        for (TrackedPeer probe : peers) {
            long lastHeard = probe.lastRoundTripNanos;
            if (lastHeard != NO_CONTACT && now - lastHeard <= window) {
                inContact++;
            }
        }
        return quorum.isReached(inContact);
    }

    private void relinquishLeadership() {
        leadershipsRelinquished++;
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
        for (TrackedPeer probe : peers) {
            probe.lastRoundTripNanos = NO_CONTACT;
        }
    }

    private void resetElectionTimer() {
        electionDeadlineNanos = clock.nanoTime() + electionTimeoutMinNanos
                + (long) (random.nextDouble() * electionTimeoutSpreadNanos);
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