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
import org.lolaf.primos.*;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

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
public final class ElectionNode implements MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(ElectionNode.class);

    /**
     * No round trip has been recorded with a peer. {@link Long#MIN_VALUE} rather than -1 because
     * these are {@link MonotonicClock#nanoTime()} readings, which may legitimately be negative.
     */
    private static final long NO_CONTACT = Long.MIN_VALUE;

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

    private final List<Peer> peers = new ArrayList<>();
    private final Map<Integer, Probe> probes = new LinkedHashMap<>();
    private final PrimosListener listener;
    private final Set<Integer> peerIds = new HashSet<>();
    private final Set<Integer> votesGranted = new HashSet<>();
    private final Set<Integer> preVotesGranted = new HashSet<>();
    private final ByteBuffer sendBuffer =
            ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    @Getter
    private Role role = Role.FOLLOWER;

    @Getter
    private long currentTerm;

    @Getter
    private int votedFor = PersistentState.NO_VOTE;
    private int leaderId = -1;
    private Leader leader;
    private long electionDeadlineNanos;
    private long heartbeatDeadlineNanos;
    private long lastLeaderContactNanos;
    private long probeDeadlineNanos;
    private long probeSequence;
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
        for (Peer peer : config.allPeers()) {
            peers.add(peer);
            peerIds.add(peer.id());
            probes.put(peer.id(), new Probe(peer));
        }
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
        this.encoder = new MessageEncoder(config.applicationName(), config.nodeId(), authenticator);
        // A node answers only its configured peers, by id. A witness server cannot: its members
        // are not configured, so it has nothing to compare an id against and the authentication
        // tag is the whole of its admission control. See ClusterConfig.Builder#learnedMembership.
        this.decoder = new MessageDecoder(config.applicationName(), authenticator,
                config.isLearnedMembership() ? id -> true : peerIds::contains, drops);
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
        if (peer.id() == config.nodeId() || !peerIds.add(peer.id())) {
            return false;
        }
        peers.add(peer);
        probes.put(peer.id(), new Probe(peer));
        log.info("cluster '{}': witness {} has learned voter {} at {}:{}",
                config.applicationName(), config.nodeId(), peer.id(), peer.host(), peer.port());
        return true;
    }

    /**
     * Reads persisted state back and arms the first election timeout.
     */
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
            transport.poll(wait, this::onDatagram);
            fireDueTimers();
        } catch (Throwable failure) {
            fail(failure);
            if (failure instanceof Error) {
                throw (Error) failure;
            }
        }
    }

    /**
     * Marks the node permanently failed.
     *
     * <p>This exists for one reason: an uncaught exception that killed the loop while leaving a
     * volatile {@code LEADER} behind would be a split-brain generator. The application would go on
     * believing it is in charge, doing whatever leaders do, while the cluster elected somebody
     * else and moved on. A dead node has to look dead.
     *
     * <p>The state is set <em>before</em> anything is rethrown, so by the time a caller sees the
     * failure {@link #role()} is already safe.
     *
     * <p>Terminal on purpose. The realistic cause is a store that can no longer make a vote
     * durable, and continuing after that means voting on state this node may forget — the very
     * thing the store exists to prevent. There is no recovery in place; the process is restarted.
     */
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
        long deadline = probeDeadlineNanos;
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
    public Optional<Leader> leader() {
        return Optional.ofNullable(leader);
    }

    /**
     * The believed leader, or null when there is none, without the {@link Optional} that
     * {@link #leader()} allocates. The production loop republishes this after every turn, and a
     * turn that changed nothing should allocate nothing — this is a field read, because the
     * {@link Leader} is built once when leadership moves.
     */
    public Leader leaderOrNull() {
        return leader;
    }

    /**
     * Role and term read together, so the fencing token cannot be separated from the role.
     */
    public RoleAndTerm roleAndTerm() {
        return new RoleAndTerm(role, currentTerm);
    }

    /**
     * The single place the role changes, so every transition is reported exactly once and the
     * time-in-role clock cannot drift out of step with it.
     */
    private void setRole(Role next) {
        if (role == next) {
            return;
        }
        Role previous = role;
        role = next;
        roleChangedAtNanos = clock.nanoTime();
        listener.onRoleChanged(previous, next, currentTerm);
    }

    /**
     * The single place the believed leader changes.
     */
    private void setLeader(int next) {
        if (leaderId == next) {
            return;
        }
        leaderId = next;
        leader = identify(next);
        listener.onLeaderChanged(leader(), currentTerm);
    }

    /**
     * Turns a leader's id into the object listeners are handed.
     *
     * <p>This node itself has no {@link Peer} — a node holds no address for itself — so
     * {@link Leader#isSelf()} carries the name from the configuration instead. A peer that is not in
     * the probe table can only happen on the witness server, which learns its voters from traffic
     * and so may hear a leader's heartbeat before {@link #learnPeer} has been called for it; every
     * other node answers only configured senders, so there the lookup always succeeds.
     */
    private Leader identify(int id) {
        if (id < 0) {
            return null;
        }
        if (id == config.nodeId()) {
            return Leader.self(id);
        }
        Probe probe = probes.get(id);
        return probe == null ? Leader.byId(id) : Leader.at(probe.peer);
    }

    /**
     * An immutable snapshot of this node's counters, for {@link org.lolaf.primos.PrimosNode#stats()}.
     */
    public PrimosStats stats() {
        return new PrimosStats(role, currentTerm,
                Duration.ofNanos(clock.nanoTime() - roleChangedAtNanos),
                electionsStarted, electionsWon, handoversSent, handoversAccepted,
                departuresObserved, drops.snapshot(), peerStats());
    }

    private void onDatagram(ByteBuffer datagram) {
        if (decoder.decode(datagram, this)) {
            // Only now, with the tag verified and the sender known, may the transport learn where
            // that peer is speaking from.
            transport.confirmSender(decoder.lastSenderId());
        }
    }

    private void fireDueTimers() {
        long now = clock.nanoTime();
        if (now >= probeDeadlineNanos) {
            probeRound(now);
            probeDeadlineNanos = now + timings.probeInterval().toNanos();
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

    // ---------------------------------------------------------------- elections

    /**
     * Asks whether a campaign would succeed, without starting one.
     *
     * <p>This is what stops a node that has been away from disrupting a healthy cluster. Without
     * it, a node partitioned for a while keeps timing out and incrementing its term — 5, 6, 7, …
     * 50 — and the moment the partition heals it sends a vote request carrying that inflated term.
     * The healthy leader sees a higher term, steps down, and the cluster holds a pointless
     * election. For an active/passive pair that is a failover nobody asked for.
     *
     * <p>So nothing is incremented and nothing is written here: the node asks peers whether they
     * <em>would</em> vote for it in the next term, and only if a majority say yes does it start a
     * real election. A node with no route to the cluster therefore never advances its term at all,
     * and rejoining is silent.
     *
     * <p>Costs no fsync, on either side of the exchange.
     */
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
        for (Peer peer : peers) {
            send(peer.id(), encoder.requestVote(sendBuffer, preVoteTerm, true));
        }
    }

    /**
     * Whether this node would vote for a candidate proposing {@code proposedTerm}.
     *
     * <p>Refused while a leader is being heard from, which is the whole point: a cluster that is
     * working says no, and the returning node's term never moves.
     */
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
     * Becomes a candidate for the next term.
     *
     * <p>The term and the self-vote are persisted <em>before</em> a single request goes out. Doing
     * it the other way round would let a node campaign in a term it might forget.
     */
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
        for (Peer peer : peers) {
            send(peer.id(), encoder.requestVote(sendBuffer, currentTerm, false));
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
        for (Probe probe : probes.values()) {
            probe.lastRoundTripNanos = now;
        }

        log.info("cluster '{}': node {} became leader for term {}",
                config.applicationName(), config.nodeId(), currentTerm);

        broadcastHeartbeat();
        heartbeatDeadlineNanos = clock.nanoTime() + timings.heartbeatInterval().toNanos();
    }

    /**
     * Adopts a higher term and returns to being a follower.
     *
     * <p>The vote is cleared because it belonged to the old term, and both are persisted before
     * this node answers anything in the new one.
     */
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
        for (Peer peer : peers) {
            send(peer.id(), encoder.appendEntries(sendBuffer, currentTerm));
        }
    }

    private void logVote(String message, Object... args) {
        if (config.isVoteLogging()) {
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
            Probe probe = probes.get(senderId);
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
     *
     * <p><strong>This deliberately skips the pre-vote round</strong>, which is the only place in
     * primos that does, so the reason has to be stated. A pre-vote asks the other followers
     * whether they <em>would</em> elect this node, and they answer no while they can still hear a
     * leader — which, at the moment a leader hands over, they all can, because it was heartbeating
     * until a moment ago. Pre-voting here would therefore be refused for a full election timeout,
     * which is exactly the wait this message exists to remove.
     *
     * <p>Skipping it is safe because pre-vote defends against a <em>different</em> thing: a
     * partitioned node inflating its term unilaterally and disrupting a healthy cluster on its
     * return. Here the term bump is authorised by the leader of the current term, which is
     * standing down as it sends this. Nothing about vote counting changes — the new candidate
     * still has to win a real majority in the new term — so the at-most-one-leader-per-term
     * argument is untouched.
     *
     * <p>A forged one of these could force an election, but forging requires the cluster secret,
     * and an attacker who has it can already inject an {@code AppendEntries} carrying an enormous
     * term, which is strictly worse. This adds no exposure that authentication was not already
     * the answer to.
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
     *
     * <p><strong>Why this message exists at all.</strong> Every other way of learning a node has
     * gone is silence, and silence has to be given a long benefit of the doubt on the JVM: a
     * stop-the-world pause is indistinguishable from a dead process, so the probe timeout gets
     * tuned up — often to tens of seconds — precisely so a garbage collection is not reported as a
     * failure. That tuning is correct, and it means a node that shut down politely would be
     * reported as alive for that whole window. This closes exactly that gap, and only that one: a
     * shutdown is the one departure the departing node knows about in advance and can say out
     * loud. Crashes, kills and partitions still take the probe timeout, as they must.
     *
     * <p><strong>What it deliberately does not do.</strong> It touches the probe state and nothing
     * else — not the election timer, not the leader id, not {@code lastRoundTripNanos}, and above
     * all not the voter count. Quorum is computed from configured membership and never from who
     * happens to be reachable; a cluster of five with two gone still needs three votes. Reading a
     * departure as "the cluster shrank" is how a split brain gets talked into existence.
     *
     * <p>Leaving {@code lastRoundTripNanos} alone is the sharp one. CheckQuorum decides whether a
     * leader may keep leading from that map, so if this cleared it, a forged or replayed departure
     * would be a way to make a healthy leader stand down. Contact that genuinely happened stays
     * recorded, and goes stale on its own schedule.
     *
     * <p>What a forged one can do is therefore bounded: mark a live peer unreachable until its
     * next probe reply arrives, which self-corrects within one probe interval. It cannot move a
     * term, spend a vote, or make a leader.
     */
    @Override
    public void onLeaving(int senderId, long term) {
        if (term > currentTerm) {
            // The universal rule, applied here as everywhere else. Nothing below depends on it.
            stepDown(term);
        }
        Probe probe = probes.get(senderId);
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
     *
     * <p>Sent by <em>any</em> node stopping cleanly, whatever its role — unlike the handover
     * above, which only a leader has anything to send. A follower's departure costs the cluster
     * nothing, but being told about it promptly is the difference between a status page that is
     * right and one that is thirty seconds stale.
     *
     * <p>Best-effort, like the handover: one datagram per peer, nothing waits for it, and if it is
     * lost the probe timeout notices in the ordinary way.
     *
     * @return how many peers were told
     */
    public int announceDeparture() {
        if (peers.isEmpty()) {
            return 0;
        }
        for (Peer peer : peers) {
            send(peer.id(), encoder.leaving(sendBuffer, currentTerm));
        }
        log.info("cluster '{}': node {} told {} peer(s) it is shutting down",
                config.applicationName(), config.nodeId(), peers.size());
        return peers.size();
    }

    /**
     * Hands leadership to a follower on the way out, instead of just going quiet.
     *
     * <p>A leader that simply stops costs the cluster up to a full election timeout with nobody in
     * charge: the followers have to notice the heartbeats stopped, and the timeout is what
     * "notice" means. Telling one of them to campaign now collapses that to about a round trip.
     *
     * <p>Best-effort by design. The message is one datagram and nothing waits for it — if it is
     * lost, or no follower is reachable, the cluster falls back to the ordinary election timeout,
     * which is exactly what would have happened anyway. There is therefore no abort path and
     * nothing to get wrong: this can make a shutdown faster and cannot make it slower.
     *
     * @return whether a handover was sent
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

    /**
     * The follower to hand over to: the one this leader most recently completed a round trip with.
     *
     * <p><strong>Never the witness</strong>, which would ignore the message anyway but must not be
     * asked — declining to campaign is the whole of what makes a witness one, and targeting one
     * would waste the handover and leave the cluster waiting out its timeout after all.
     *
     * <p>The signal is {@code lastRoundTripNanos}, the same one CheckQuorum uses, rather than the
     * probe's reachability. Both were tried and the difference matters: probes run once a second,
     * so a leader elected moments ago has no probe results at all and would find nobody to hand to,
     * while an {@code AppendEntriesReply} lands every heartbeat and is direct proof that this
     * follower received something this node sent and answered it. Handing over to a node that
     * cannot hear us is a datagram into a void, and picking well is the entire job here.
     *
     * <p>Peers the probe has actually declared down are still excluded. A leader seeds every peer's
     * contact clock when it takes office, deliberately — see {@link #becomeLeader} — so straight
     * after an election that clock alone would vouch for a peer nothing has heard from.
     */
    private Peer handoverTarget() {
        long now = clock.nanoTime();
        long window = timings.electionTimeoutMin().toNanos();
        Peer best = null;
        long bestContactNanos = Long.MIN_VALUE;

        for (Probe probe : probes.values()) {
            if (probe.peer.isWitness()) {
                continue;
            }
            if (probe.consecutiveMisses >= timings.failureThreshold()) {
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
        Probe probe = probes.get(senderId);
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

    // ---------------------------------------------------------------- plumbing

    /**
     * Probes every peer, and calls down anything that has missed too many in a row.
     *
     * <p>Every node probes every peer, rather than the leader reporting who it can see. The
     * obvious cheaper design — piggybacking a reachability bitmap on the heartbeat — was rejected
     * because it goes blind exactly when there is no leader, which is precisely the outage during
     * which somebody is asking who is up. It also costs nothing to say honestly: each node reports
     * what <em>it</em> can reach, rather than relaying an opinion formed elsewhere.
     *
     * <p>Probing is separate from the heartbeat and much slower. Heartbeats are fast because they
     * suppress election timers; reachability reporting is not, and a witness on another network
     * would otherwise pay for a 50ms heartbeat it does not need.
     */
    private void probeRound(long now) {
        for (Probe probe : probes.values()) {
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

    private void notifyPeerUp(Peer peer) {
        log.info("cluster '{}': {} {} is reachable",
                config.applicationName(), peer.isWitness() ? "witness" : "peer", peer.id());
        listener.onPeerUp(peer);
    }

    private void notifyPeerDown(Peer peer) {
        if (peer.isWitness()) {
            log.warn("cluster '{}': the witness ({}) is unreachable. Nothing looks broken, but"
                            + " this cluster can no longer survive a partition.",
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
        for (Probe probe : probes.values()) {
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
        Probe probe = probes.get(peerId);
        return probe != null && probe.reachable;
    }

    /**
     * Whether this leader has completed a round trip with a majority recently enough.
     *
     * <p><strong>Without this a partitioned leader would report {@code LEADER} forever.</strong>
     * Plain Raft lets it: an isolated leader keeps sending heartbeats into the void and never
     * learns of the term that replaced it. That is safe in real Raft, where the log defends
     * itself — nothing can be committed without a majority. primos has no log, so the only thing
     * a leader produces is the answer to <em>am I the leader?</em>, and an isolated node answering
     * yes while the other side elects a new one is precisely the split brain this library exists
     * to prevent. So the leader stands itself down.
     *
     * <p>Contact means an {@code AppendEntriesReply}, and nothing else, deliberately. Counting any
     * inbound message would let a **one-way** partition keep a leader alive: it would hear the
     * peers fine while none of its own heartbeats arrived. A reply proves the peer received
     * something this node sent.
     *
     * <p>The window is the minimum election timeout, so a leader gives up its claim no later than
     * the earliest moment any follower could start campaigning for a replacement.
     *
     * <p>This bounds the stale-leader window; it cannot close it. Between losing contact and
     * noticing, this node still answers yes. That residue is what the fencing token is for.
     */
    private boolean hasQuorumContact(long now) {
        long window = timings.electionTimeoutMin().toNanos();
        int inContact = 1;
        for (Probe probe : probes.values()) {
            long lastHeard = probe.lastRoundTripNanos;
            if (lastHeard != NO_CONTACT && now - lastHeard <= window) {
                inContact++;
            }
        }
        return quorum.isReached(inContact);
    }

    /**
     * Gives up leadership without advancing the term.
     *
     * <p>Nothing durable changes: the term is the same and the vote this node cast for itself in
     * it still stands. So there is nothing to persist and no fsync on this path.
     */
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

    /**
     * Forgets every round trip this node recorded while it was leading.
     *
     * <p>Called only where leadership has just ended, so the next one starts from a clean slate
     * instead of inheriting contact the previous term established.
     */
    private void forgetQuorumContact() {
        for (Probe probe : probes.values()) {
            probe.lastRoundTripNanos = NO_CONTACT;
        }
    }

    /**
     * A fresh timeout drawn from the configured range on every reset.
     *
     * <p>Randomising this is not a refinement, it is what makes elections terminate: nodes that
     * time out together campaign together, split the vote and repeat.
     */
    private void resetElectionTimer() {
        long min = timings.electionTimeoutMin().toNanos();
        long spread = timings.electionTimeoutSpread().toNanos();
        electionDeadlineNanos = clock.nanoTime() + min + (long) (random.nextDouble() * spread);
    }

    private void persist() {
        store.save(currentTerm, votedFor);
    }

    /**
     * Sends without ever letting a network failure become a node failure. UDP sends fail routinely
     * and the algorithm's own timers are the retry mechanism.
     */
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

    /**
     * Peers this node exchanges messages with, the witness included.
     */
    List<Peer> peers() {
        return new ArrayList<>(peers);
    }

    /**
     * What this node knows about reaching one peer.
     */
    private static final class Probe {

        private final Peer peer;

        private long outstandingSequence = -1;
        private long sentAtNanos;
        private boolean awaitingReply;
        private int consecutiveMisses;
        private boolean reachable;
        private long lastProbeRoundTripNanos = -1;
        private long lastHeardNanos = -1;

        /**
         * When this node, while leading, last completed a round trip with this peer — the arrival
         * of an {@code AppendEntriesReply}, and nothing else ever. CheckQuorum decides from it
         * whether a leader may keep leading, so anything else writing it becomes a way to depose a
         * healthy leader with a forged datagram. It lives here, one long per peer, rather than in a
         * map of boxed values the leader would rewrite on every heartbeat reply.
         *
         * <p>Not {@link #lastProbeRoundTripNanos} above, which is how long a probe took rather than
         * when contact last happened.
         */
        private long lastRoundTripNanos = NO_CONTACT;

        private Probe(Peer peer) {
            this.peer = peer;
        }
    }
}
