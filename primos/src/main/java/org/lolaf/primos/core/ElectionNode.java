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
import java.util.HashSet;
import java.util.List;
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
    private final ByteBuffer sendBuffer =
            ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private int votedFor = PersistentState.NO_VOTE;
    private int leaderId = -1;
    private long electionDeadlineNanos;
    private long heartbeatDeadlineNanos;
    private boolean started;

    private long electionsStarted;
    private long electionsWon;

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
            if (now >= heartbeatDeadlineNanos) {
                broadcastHeartbeat();
                heartbeatDeadlineNanos = now + timings.heartbeatInterval().toNanos();
            }
            return;
        }
        if (!config.isWitness() && now >= electionDeadlineNanos) {
            startElection();
        }
    }

    // ---------------------------------------------------------------- elections

    /**
     * Becomes a candidate for the next term.
     *
     * <p>The term and the self-vote are persisted <em>before</em> a single request goes out. Doing
     * it the other way round would let a node campaign in a term it might forget.
     */
    private void startElection() {
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
        role = Role.LEADER;
        leaderId = config.nodeId();
        electionsWon++;
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
        role = Role.FOLLOWER;
        leaderId = senderId;
        votesGranted.clear();
        resetElectionTimer();

        send(senderId, encoder.appendEntriesReply(sendBuffer, currentTerm, true));
    }

    @Override
    public void onAppendEntriesReply(int senderId, long term, boolean success) {
        if (term > currentTerm) {
            stepDown(term);
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
