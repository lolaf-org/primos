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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;

import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Quorum;
import org.lolaf.primos.Role;
import org.lolaf.primos.RoleAndTerm;
import org.lolaf.primos.Timings;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.DropCounters;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageDecoder;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.MessageHandler;
import org.lolaf.primos.wire.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final ClusterConfig config;
    private final Timings timings;
    private final Quorum quorum;
    private final StateStore store;
    private final Transport transport;
    private final MonotonicClock clock;
    private final Random random;
    private final MessageEncoder encoder;
    private final MessageDecoder decoder;
    private final DropCounters drops = new DropCounters();

    private final List<Peer> peers;
    private final Set<Integer> peerIds = new HashSet<>();
    private final Set<Integer> votesGranted = new HashSet<>();
    private final Set<Integer> preVotesGranted = new HashSet<>();
    private final Map<Integer, Long> lastRoundTripNanos = new HashMap<>();
    private final ByteBuffer sendBuffer =
            ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private int votedFor = PersistentState.NO_VOTE;
    private int leaderId = -1;
    private long electionDeadlineNanos;
    private long heartbeatDeadlineNanos;
    private long lastLeaderContactNanos;
    private boolean preVoting;
    private long preVoteTerm;
    private boolean started;

    private long preVotesStarted;
    private long electionsStarted;
    private long electionsWon;
    private long leadershipsRelinquished;

    public ElectionNode(ClusterConfig config,
                        StateStore store,
                        Transport transport,
                        MonotonicClock clock,
                        Random random,
                        MessageAuthenticator authenticator) {
        this.config = Objects.requireNonNull(config, "config");
        this.timings = config.timings();
        this.quorum = Quorum.of(config);
        this.store = Objects.requireNonNull(store, "store");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.peers = config.allPeers();
        for (Peer peer : peers) {
            peerIds.add(peer.id());
        }
        this.encoder = new MessageEncoder(config.applicationName(), config.nodeId(), authenticator);
        this.decoder = new MessageDecoder(config.applicationName(), authenticator,
                peerIds::contains, drops);
    }

    /** Reads persisted state back and arms the first election timeout. */
    public void start() {
        if (started) {
            throw new IllegalStateException("node " + config.nodeId() + " is already started");
        }
        started = true;

        PersistentState state = store.load();
        currentTerm = state.currentTerm();
        votedFor = state.votedFor();
        role = Role.FOLLOWER;

        log.info("{}", quorum.summary());
        for (String warning : quorum.warnings()) {
            log.warn("{}", warning);
        }
        if (!store.isDurable()) {
            log.warn("cluster '{}': node {} is using a state store that does not survive a crash."
                            + " After any restart this node may vote twice in one term, which can"
                            + " elect two leaders. Use this only for tests.",
                    config.applicationName(), config.nodeId());
        }
        lastLeaderContactNanos = clock.nanoTime();
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
        long wait = Math.min(maxWaitNanos, Math.max(0L, nextDeadlineNanos() - clock.nanoTime()));
        transport.poll(wait, this::onDatagram);
        fireDueTimers();
    }

    /**
     * When this node next has something to do.
     *
     * <p>A witness returns {@link Long#MAX_VALUE}: it never campaigns, so it has no election timer
     * and nothing to do until someone talks to it.
     */
    public long nextDeadlineNanos() {
        if (role == Role.LEADER) {
            return heartbeatDeadlineNanos;
        }
        return config.isWitness() ? Long.MAX_VALUE : electionDeadlineNanos;
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public int votedFor() {
        return votedFor;
    }

    /** Which node is currently believed to be leader, if any. */
    public OptionalInt leaderId() {
        return leaderId < 0 ? OptionalInt.empty() : OptionalInt.of(leaderId);
    }

    /** Role and term read together, so the fencing token cannot be separated from the role. */
    public RoleAndTerm roleAndTerm() {
        return new RoleAndTerm(role, currentTerm);
    }

    public ClusterConfig config() {
        return config;
    }

    public DropCounters drops() {
        return drops;
    }

    /** How many pre-vote rounds this node has run, whether or not they led to an election. */
    public long preVotesStarted() {
        return preVotesStarted;
    }

    public long electionsStarted() {
        return electionsStarted;
    }

    public long electionsWon() {
        return electionsWon;
    }

    private void onDatagram(ByteBuffer datagram) {
        decoder.decode(datagram, this);
    }

    private void fireDueTimers() {
        long now = clock.nanoTime();
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

        role = Role.CANDIDATE;
        leaderId = -1;
        votesGranted.clear();
        votesGranted.add(config.nodeId());
        electionsStarted++;
        resetElectionTimer();

        log.debug("node {} started an election for term {}", config.nodeId(), currentTerm);

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
        role = Role.LEADER;
        leaderId = config.nodeId();
        electionsWon++;

        // Start the contact clock now, or a new leader would step down before its first heartbeat
        // had a chance to be answered.
        long now = clock.nanoTime();
        for (Peer peer : peers) {
            lastRoundTripNanos.put(peer.id(), now);
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
        role = Role.FOLLOWER;
        leaderId = -1;
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

    // ---------------------------------------------------------------- messages

    @Override
    public void onRequestVote(int senderId, long term, boolean preVote) {
        if (preVote) {
            // Deliberately no step-down, no persistence and no timer reset. A pre-vote must be
            // answerable without changing anything, or it would inflate terms exactly as the real
            // vote it is meant to avoid, and a node could suppress elections by spamming them.
            boolean would = wouldGrantVote(term, clock.nanoTime());
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
            if (preVotesGranted.add(senderId) && quorum.isReached(preVotesGranted.size())) {
                startElection();
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
        if (votesGranted.add(senderId) && quorum.isReached(votesGranted.size())) {
            becomeLeader();
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
        role = Role.FOLLOWER;
        leaderId = senderId;
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
            lastRoundTripNanos.put(senderId, clock.nanoTime());
        }
    }

    @Override
    public void onPing(int senderId, long term, long sequence) {
        if (term > currentTerm) {
            stepDown(term);
        }
        send(senderId, encoder.pingReply(sendBuffer, currentTerm, sequence));
    }

    @Override
    public void onPingReply(int senderId, long term, long sequence) {
        if (term > currentTerm) {
            stepDown(term);
        }
    }

    // ---------------------------------------------------------------- plumbing

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
        for (Peer peer : peers) {
            Long lastHeard = lastRoundTripNanos.get(peer.id());
            if (lastHeard != null && now - lastHeard <= window) {
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

        role = Role.FOLLOWER;
        leaderId = -1;
        votesGranted.clear();
        lastRoundTripNanos.clear();
        resetElectionTimer();
    }

    /** How many times this node has stood itself down for lack of quorum contact. */
    public long leadershipsRelinquished() {
        return leadershipsRelinquished;
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

    /** Peers this node exchanges messages with, the witness included. */
    List<Peer> peers() {
        return new ArrayList<>(peers);
    }
}
