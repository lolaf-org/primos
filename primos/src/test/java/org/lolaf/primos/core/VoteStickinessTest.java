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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Role;
import org.lolaf.primos.Timings;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.SimulatedNetwork;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.WireFormat;

/**
 * A real vote honours the leader stickiness a pre-vote does, or a pre-vote granted before a heartbeat elects a
 * successor during the lease that heartbeat renewed. A real voter, real datagrams.
 */
class VoteStickinessTest {

    private static final Duration WINDOW = Timings.lan().electionTimeoutMin();
    private static final int LEADER = 2;
    private static final int CANDIDATE = 3;
    private static final int OTHER_CANDIDATE = 4;
    private static final long TERM = 5;

    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network = new SimulatedNetwork(1L, clock);
    private final StateStore store = new MemoryStateStore();
    private ElectionNode voter;

    @BeforeEach
    void startVoterFollowingALeader() {
        network.delayRange(0, 0);
        store.save(TERM, PersistentState.NO_VOTE);
        voter = new ElectionNode(ClusterConfig.builder().applicationName("t").nodeId(1)
                .peers(List.of(peer(LEADER), peer(CANDIDATE), peer(OTHER_CANDIDATE), peer(5))).build(),
                store, network.transportFor(1), clock, new Random(1L), MessageAuthenticator.disabled());
        voter.start();
    }

    @Test
    void a_vote_arriving_just_after_a_heartbeat_is_refused_and_changes_nothing() {
        waitOutTheWindow();
        assertThat(vote(CANDIDATE, TERM + 1, true).granted()).as("the pre-vote round passes").isTrue();
        heartbeat();

        CapturedReply reply = vote(CANDIDATE, TERM + 1, false);

        assertThat(reply.granted()).isFalse();
        assertThat(voter.currentTerm()).as("a refused candidate must not move the term").isEqualTo(TERM);
        assertThat(voter.votedFor()).isEqualTo(PersistentState.NO_VOTE);
        assertThat(store.load().votedFor()).isEqualTo(PersistentState.NO_VOTE);
        assertThat(voter.leaderOptional().get().id()).isEqualTo(LEADER);
    }

    @Test
    void the_same_vote_is_granted_once_the_window_has_passed() {
        heartbeat();
        clock.advance(WINDOW);

        assertThat(vote(CANDIDATE, TERM + 1, false).granted()).isTrue();
        assertThat(voter.currentTerm()).isEqualTo(TERM + 1);
        assertThat(store.load().votedFor()).isEqualTo(CANDIDATE);
    }

    @Test
    void the_window_is_refused_up_to_its_last_instant() {
        heartbeat();
        clock.advance(WINDOW.minusNanos(1));

        assertThat(vote(CANDIDATE, TERM + 1, false).granted()).isFalse();
    }

    @Test
    void a_handover_is_granted_just_after_a_heartbeat() {
        heartbeat();

        CapturedReply reply = send(CANDIDATE, (encoder, buffer) -> encoder.handoverVote(buffer, TERM + 1));

        assertThat(reply.granted()).as("the leader asked for this itself").isTrue();
        assertThat(voter.currentTerm()).isEqualTo(TERM + 1);
    }

    @Test
    void a_handover_does_not_buy_a_second_vote_in_a_term() {
        waitOutTheWindow();
        assertThat(vote(OTHER_CANDIDATE, TERM + 1, false).granted()).isTrue();

        CapturedReply reply = send(CANDIDATE, (encoder, buffer) -> encoder.handoverVote(buffer, TERM + 1));

        assertThat(reply.granted()).isFalse();
        assertThat(store.load().votedFor()).isEqualTo(OTHER_CANDIDATE);
    }

    @Test
    void a_duplicate_of_a_vote_already_granted_is_granted_again() {
        waitOutTheWindow();
        assertThat(vote(CANDIDATE, TERM + 1, false).granted()).isTrue();

        assertThat(vote(CANDIDATE, TERM + 1, false).granted())
                .as("UDP duplicates, and a grant counts as leader contact")
                .isTrue();
        assertThat(store.load().votedFor()).isEqualTo(CANDIDATE);
    }

    @Test
    void a_grant_holds_off_a_rival_for_the_window() {
        waitOutTheWindow();
        assertThat(vote(CANDIDATE, TERM + 1, false).granted()).isTrue();

        assertThat(vote(OTHER_CANDIDATE, TERM + 2, false).granted())
                .as("the lease of the node this voter just elected counts on it")
                .isFalse();
        assertThat(voter.currentTerm()).isEqualTo(TERM + 1);
    }

    @Test
    void a_restarted_voter_refuses_for_the_window() {
        assertThat(vote(CANDIDATE, TERM + 1, false).granted())
                .as("it cannot know whom it heard from before the restart")
                .isFalse();
    }

    @Test
    void a_stale_candidate_is_told_the_current_term() {
        waitOutTheWindow();

        CapturedReply reply = vote(CANDIDATE, TERM - 1, false);

        assertThat(reply.granted()).isFalse();
        assertThat(reply.replyTerm()).isEqualTo(TERM);
    }

    @Test
    void the_flag_is_its_own_bit() {
        assertThat(WireFormat.FLAG_HANDOVER & (WireFormat.FLAG_PRE_VOTE | WireFormat.FLAG_DECLINES_CANDIDACY)).isZero();
    }

    private void waitOutTheWindow() {
        clock.advance(WINDOW);
        voter.runOnce(0L);
        drainCandidates();
    }

    private void heartbeat() {
        network.transportFor(LEADER).send(1, encoderOf(LEADER).appendEntries(buffer(), TERM, 0, 0));
        voter.runOnce(0L);
        assertThat(voter.role()).isEqualTo(Role.FOLLOWER);
        drainCandidates();
    }

    private CapturedReply vote(int from, long term, boolean preVote) {
        return send(from, (encoder, buffer) -> encoder.requestVote(buffer, term, preVote));
    }

    private CapturedReply send(int from, BiFunction<MessageEncoder, ByteBuffer, ByteBuffer> message) {
        drainCandidates();
        network.transportFor(from).send(1, message.apply(encoderOf(from), buffer()));
        voter.runOnce(0L);
        return new CapturedReply("t").drain(network.transportFor(from));
    }

    private void drainCandidates() {
        new CapturedReply("t").drain(network.transportFor(CANDIDATE)).drain(network.transportFor(OTHER_CANDIDATE));
    }

    private static MessageEncoder encoderOf(int id) {
        return new MessageEncoder("t", id, MessageAuthenticator.disabled());
    }

    private static ByteBuffer buffer() {
        return ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
    }

    private static Peer peer(int id) {
        return Peer.application(id, "sim", 7000 + id);
    }
}
