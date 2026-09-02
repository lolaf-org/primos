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

import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Leader;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Role;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.SimulatedNetwork;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.WireFormat;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One node in isolation, for the two properties that are easiest to prove by reaching straight
 * into it rather than by waiting for a cluster to misbehave.
 */
class ElectionNodeTest {

    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network = new SimulatedNetwork(1L, clock);
    private final StateStore store = new MemoryStateStore();

    private static List<Peer> peers(int... ids) {
        List<Peer> peers = new ArrayList<>();
        for (int id : ids) {
            peers.add(Peer.application(id, "sim", 7000 + id));
        }
        return peers;
    }

    @Test
    void the_same_grant_arriving_five_times_is_still_one_vote() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();

        for (int i = 0; i < 5; i++) {
            node.onRequestVoteReply(2, term, true, false);
        }

        assertThat(node.role())
                .as("a set, not a counter: five copies of node 2's grant is two votes of five,"
                        + " which is not the three a quorum needs")
                .isEqualTo(Role.CANDIDATE);
    }

    @Test
    void grants_from_genuinely_different_voters_do_elect_a_leader() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();

        node.onRequestVoteReply(2, term, true, false);
        assertThat(node.role()).isEqualTo(Role.CANDIDATE);

        node.onRequestVoteReply(3, term, true, false);

        assertThat(node.role()).isEqualTo(Role.LEADER);
        assertThat(node.currentTerm()).isEqualTo(term);
    }

    @Test
    void refusals_never_count_however_many_arrive() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();

        for (int voter = 2; voter <= 5; voter++) {
            node.onRequestVoteReply(voter, term, false, false);
        }

        assertThat(node.role()).isEqualTo(Role.CANDIDATE);
    }

    @Test
    void a_grant_from_a_term_already_left_is_ignored() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();

        node.onRequestVoteReply(2, term - 1, true, false);
        node.onRequestVoteReply(3, term - 1, true, false);

        assertThat(node.role())
                .as("late replies from a previous term are routine on UDP and never interesting")
                .isEqualTo(Role.CANDIDATE);
    }

    @Test
    void a_vote_granted_before_a_crash_is_still_binding_after_it() {
        ElectionNode node = startedNode();
        node.onRequestVote(2, 5, false);

        assertThat(node.currentTerm()).isEqualTo(5);
        assertThat(node.votedFor()).isEqualTo(2);

        ElectionNode restarted = startedNode();

        assertThat(restarted.currentTerm())
                .as("the vote was persisted before the reply went out, so it survives")
                .isEqualTo(5);
        assertThat(restarted.votedFor()).isEqualTo(2);

        restarted.onRequestVote(3, 5, false);

        assertThat(restarted.votedFor())
                .as("granting a second vote in term 5 would let two candidates each win")
                .isEqualTo(2);
    }

    @Test
    void a_second_request_from_the_same_candidate_is_answered_the_same_way() {
        ElectionNode node = startedNode();

        node.onRequestVote(2, 5, false);
        node.onRequestVote(2, 5, false);

        assertThat(node.votedFor())
                .as("a duplicated request must be idempotent, not a refusal")
                .isEqualTo(2);
    }

    @Test
    void a_higher_term_clears_the_vote_that_belonged_to_the_old_one() {
        ElectionNode node = startedNode();
        node.onRequestVote(2, 5, false);
        assertThat(node.votedFor()).isEqualTo(2);

        node.onAppendEntries(3, 6);

        assertThat(node.currentTerm()).isEqualTo(6);
        assertThat(node.votedFor()).isEqualTo(PersistentState.NO_VOTE);
        assertThat(node.leaderOptional()).map(Leader::id).hasValue(3);
        assertThat(node.leaderOptional().flatMap(Leader::peer))
                .as("a follower is told where the leader is, so it need not look the id up to"
                        + " forward work there")
                .contains(Peer.application(3, "sim", 7003));
        assertThat(node.leaderOptional().get().isSelf()).isFalse();
    }

    @Test
    void a_node_leading_itself_says_so_rather_than_looking_like_no_leader() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();
        node.onRequestVoteReply(2, term, true, false);
        node.onRequestVoteReply(3, term, true, false);

        assertThat(node.role()).isEqualTo(Role.LEADER);

        Leader leader = node.leaderOptional().orElseThrow(AssertionError::new);
        assertThat(leader.id()).isEqualTo(1);
        assertThat(leader.isSelf())
                .as("this node holds it, which is the opposite of nobody holding it and must not"
                        + " share the empty encoding")
                .isTrue();
        assertThat(leader.peer())
                .as("a node holds no address for itself, and there is nowhere to forward to")
                .isEmpty();
    }

    @Test
    void a_witness_hearing_a_leader_it_has_not_learned_yet_knows_only_the_id() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(99)
                .asWitness()
                .learnedMembership()
                .build();
        ElectionNode witness = new ElectionNode(config, store, network.transportFor(99), clock,
                new Random(1L), MessageAuthenticator.disabled());
        witness.start();

        // The witness server learns voters between turns of its own loop, so a heartbeat can
        // arrive before learnPeer has been called for its sender. Every other node answers only
        // configured senders, so this is the one place the peer can be missing.
        witness.onAppendEntries(7, 1);

        Leader leader = witness.leaderOptional().orElseThrow(AssertionError::new);
        assertThat(leader.id()).isEqualTo(7);
        assertThat(leader.isSelf()).isFalse();
        assertThat(leader.peer()).isEmpty();
        assertThat(leader.name()).isEqualTo("node-7");
    }

    @Test
    void a_heartbeat_from_a_stale_leader_is_refused_and_does_not_move_the_term() {
        ElectionNode node = startedNode();
        node.onRequestVote(2, 7, false);

        node.onAppendEntries(3, 4);

        assertThat(node.currentTerm()).isEqualTo(7);
        assertThat(node.leaderOptional()).isEmpty();
    }

    @Test
    void a_candidate_steps_down_when_it_hears_from_a_leader_of_its_own_term() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();

        node.onAppendEntries(4, term);

        assertThat(node.role()).isEqualTo(Role.FOLLOWER);
        assertThat(node.leaderOptional()).map(Leader::id).hasValue(4);
    }

    @Test
    void a_witness_never_arms_an_election_timer() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(99)
                .asWitness()
                .peers(peers(1, 2))
                .build();
        ElectionNode witness = new ElectionNode(config, store, network.transportFor(99), clock,
                new Random(1L), MessageAuthenticator.disabled());
        witness.start();

        clock.advance(Duration.ofMinutes(10));
        witness.runOnce(0L);

        assertThat(witness.role()).isEqualTo(Role.FOLLOWER);
        assertThat(witness.electionsStarted())
                .as("ten minutes without a leader and it still has not campaigned")
                .isZero();
        assertThat(witness.preVotesStarted())
                .as("it does not even ask whether it could")
                .isZero();
        assertThat(witness.nextDeadlineNanos())
                .as("it still probes its peers, so it has a deadline; what it lacks is an"
                        + " election timer")
                .isLessThan(Long.MAX_VALUE);
    }

    // ------------------------------------------------------------------ pre-vote

    @Test
    void a_witness_still_grants_votes() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(99)
                .asWitness()
                .peers(peers(1, 2))
                .build();
        ElectionNode witness = new ElectionNode(config, store, network.transportFor(99), clock,
                new Random(1L), MessageAuthenticator.disabled());
        witness.start();

        witness.onRequestVote(1, 3, false);

        assertThat(witness.votedFor()).isEqualTo(1);
        assertThat(witness.currentTerm()).isEqualTo(3);
    }

    @Test
    void a_pre_vote_is_refused_while_a_leader_is_being_heard_from() {
        ElectionNode node = startedNode();
        node.onAppendEntries(2, 1);

        node.onRequestVote(3, 99, true);

        CapturedReply reply = new CapturedReply("t").drain(network.transportFor(3));
        assertThat(reply.granted())
                .as("a working cluster says no, which is what stops a returning node disrupting it")
                .isFalse();
        assertThat(reply.preVote()).isTrue();
    }

    @Test
    void a_pre_vote_is_granted_once_no_leader_has_been_heard_from_for_an_election_timeout() {
        ElectionNode node = startedNode();
        node.onAppendEntries(2, 1);
        clock.advance(Duration.ofSeconds(1));

        node.onRequestVote(3, 2, true);

        assertThat(new CapturedReply("t").drain(network.transportFor(3)).granted()).isTrue();
    }

    @Test
    void answering_a_pre_vote_changes_nothing_at_all() {
        ElectionNode node = startedNode();
        clock.advance(Duration.ofSeconds(1));
        long termBefore = node.currentTerm();

        node.onRequestVote(3, 500, true);

        assertThat(node.currentTerm())
                .as("a pre-vote carrying a huge term must not inflate ours: that is the whole point")
                .isEqualTo(termBefore);
        assertThat(node.votedFor()).isEqualTo(PersistentState.NO_VOTE);
        assertThat(node.role()).isEqualTo(Role.FOLLOWER);
        assertThat(store.load().currentTerm())
                .as("and it must not have cost an fsync")
                .isEqualTo(termBefore);
    }

    @Test
    void a_leader_refuses_pre_votes() {
        ElectionNode node = campaigningNodeOfFive();
        node.onRequestVoteReply(2, node.currentTerm(), true, false);
        node.onRequestVoteReply(3, node.currentTerm(), true, false);
        assertThat(node.role()).isEqualTo(Role.LEADER);

        node.onRequestVote(4, node.currentTerm() + 1, true);

        assertThat(new CapturedReply("t").drain(network.transportFor(4)).granted()).isFalse();
    }

    @Test
    void the_reply_echoes_the_proposed_term_so_an_abandoned_round_cannot_be_counted() {
        ElectionNode node = startedNode();
        clock.advance(Duration.ofSeconds(1));

        node.onRequestVote(3, 42, true);

        assertThat(new CapturedReply("t").drain(network.transportFor(3)).replyTerm())
                .isEqualTo(42);
    }

    @Test
    void a_pre_vote_reply_for_a_round_that_was_abandoned_is_ignored() {
        ElectionNode node = startedNode(peers(2, 3, 4, 5));
        clock.advance(Duration.ofSeconds(1));
        node.runOnce(0L);

        node.onRequestVoteReply(2, 99, true, true);
        node.onRequestVoteReply(3, 99, true, true);

        assertThat(node.role())
                .as("those grants were for a term this node never proposed")
                .isEqualTo(Role.FOLLOWER);
        assertThat(node.currentTerm()).isZero();
    }

    @Test
    void timing_out_starts_a_pre_vote_rather_than_an_election() {
        ElectionNode node = startedNode(peers(2, 3, 4, 5));

        clock.advance(Duration.ofSeconds(1));
        node.runOnce(0L);

        assertThat(node.preVotesStarted()).isEqualTo(1);
        assertThat(node.electionsStarted()).isZero();
        assertThat(node.currentTerm()).isZero();
        assertThat(store.load().currentTerm()).isZero();
    }

    @Test
    void a_single_node_cluster_skips_straight_through_its_own_pre_vote() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t").nodeId(1).build();
        ElectionNode node = new ElectionNode(config, store, network.transportFor(1), clock,
                new Random(1L), MessageAuthenticator.disabled());
        node.start();

        clock.advance(Duration.ofSeconds(1));
        node.runOnce(0L);

        assertThat(node.role()).isEqualTo(Role.LEADER);
        assertThat(node.currentTerm()).isEqualTo(1);
    }

    // ------------------------------------------------------------------ timing advertisements

    /**
     * A node of five that has timed out, won its pre-vote and is now genuinely campaigning, so
     * quorum is three.
     *
     * <p>Timing out no longer campaigns on its own: the node first asks whether a campaign would
     * succeed, and only a majority saying yes moves it to {@code CANDIDATE} and advances the term.
     */
    private ElectionNode campaigningNodeOfFive() {
        ElectionNode node = startedNode(peers(2, 3, 4, 5));
        clock.advance(Duration.ofSeconds(1));
        node.runOnce(0L);

        assertThat(node.role())
                .as("a pre-vote round changes nothing until it is won")
                .isEqualTo(Role.FOLLOWER);
        assertThat(node.currentTerm()).isZero();

        node.onRequestVoteReply(2, 1, true, true);
        node.onRequestVoteReply(3, 1, true, true);

        assertThat(node.role()).isEqualTo(Role.CANDIDATE);
        assertThat(node.currentTerm()).isEqualTo(1);
        assertThat(node.votedFor()).isEqualTo(1);
        return node;
    }

    /**
     * Logged and counted on the transition, not on every advert. A peer restates its intervals
     * every thirty seconds for as long as it runs, so counting datagrams would turn one
     * misconfiguration into an ever-growing number and a log line every half minute.
     */
    @Test
    void a_peer_running_different_intervals_is_reported_once_not_on_every_advert() {
        ElectionNode node = startedNode();

        advertise(2, node, 200, 1500, 3000, 2000, 3);
        assertThat(node.stats().timingMismatches()).isEqualTo(1);

        advertise(2, node, 200, 1500, 3000, 2000, 3);
        advertise(2, node, 200, 1500, 3000, 2000, 3);

        assertThat(node.stats().timingMismatches())
                .as("the same peer still being wrong is not news")
                .isEqualTo(1);
    }

    /**
     * The recovery matters as much as the error: during a rolling change half a cluster disagrees
     * for a few minutes by design, and whoever is watching needs to see it end.
     */
    @Test
    void a_peer_returning_to_the_same_intervals_can_diverge_again() {
        ElectionNode node = startedNode();

        advertise(2, node, 200, 1500, 3000, 2000, 3);
        assertThat(node.stats().timingMismatches()).isEqualTo(1);

        advertise(2, node, 50, 300, 600, 1000, 3);
        assertThat(node.stats().timingMismatches())
                .as("agreeing again is not another mismatch")
                .isEqualTo(1);

        advertise(2, node, 200, 1500, 3000, 2000, 3);
        assertThat(node.stats().timingMismatches())
                .as("but diverging afresh is")
                .isEqualTo(2);
    }

    /**
     * The whole safety argument for this message. A mismatch makes failover slower, never unsafe,
     * so acting on one would turn an ordinary rolling timing change into a partition — a worse
     * failure than the one being detected, and one nobody could roll forward out of.
     */
    @Test
    void a_mismatched_advertisement_changes_nothing_at_all() {
        ElectionNode node = startedNode();
        long termBefore = node.currentTerm();
        Role roleBefore = node.role();
        long deadlineBefore = node.nextDeadlineNanos();

        advertise(2, node, 200, 1500, 3000, 2000, 9);

        assertThat(node.currentTerm()).isEqualTo(termBefore);
        assertThat(node.role()).isEqualTo(roleBefore);
        assertThat(node.nextDeadlineNanos())
                .as("not the election timer, not the probe timer, nothing")
                .isEqualTo(deadlineBefore);
        assertThat(node.stats().timingMismatches()).isEqualTo(1);
    }

    @Test
    void a_peer_running_the_same_intervals_is_never_mentioned() {
        ElectionNode node = startedNode();

        advertise(2, node, 50, 300, 600, 1000, 3);

        assertThat(node.stats().timingMismatches())
                .as("silence is the correct output for a cluster that agrees")
                .isZero();
    }

    /**
     * Delivers a real datagram rather than calling the handler, so the decoder, the size check and
     * {@code runOnce}'s wrapping are all on the path — which is the only way this is the production
     * path rather than a shape that resembles it.
     */
    private void advertise(int fromPeer, ElectionNode to, int heartbeat, int electionMin,
                           int electionMax, int probe, int failureThreshold) {
        MessageEncoder encoder = new MessageEncoder("t", fromPeer, MessageAuthenticator.disabled());
        ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
        network.transportFor(fromPeer).send(to.config().nodeId(), encoder.nodeConfig(
                buffer, to.currentTerm(), heartbeat, electionMin, electionMax, probe,
                failureThreshold));
        to.runOnce(0);
    }

    private ElectionNode startedNode() {
        return startedNode(peers(2, 3));
    }

    private ElectionNode startedNode(List<Peer> peers) {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(1)
                .peers(peers)
                .build();
        Transport transport = network.transportFor(1);
        ElectionNode node = new ElectionNode(config, store, transport, clock,
                new Random(1L), MessageAuthenticator.disabled());
        node.start();
        return node;
    }
}
