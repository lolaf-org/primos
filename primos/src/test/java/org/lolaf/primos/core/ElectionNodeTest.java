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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Role;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.SimulatedNetwork;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;

/**
 * One node in isolation, for the two properties that are easiest to prove by reaching straight
 * into it rather than by waiting for a cluster to misbehave.
 */
class ElectionNodeTest {

    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network = new SimulatedNetwork(1L, clock);
    private final StateStore store = new MemoryStateStore();

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
        assertThat(node.leaderId()).hasValue(3);
    }

    @Test
    void a_heartbeat_from_a_stale_leader_is_refused_and_does_not_move_the_term() {
        ElectionNode node = startedNode();
        node.onRequestVote(2, 7, false);

        node.onAppendEntries(3, 4);

        assertThat(node.currentTerm()).isEqualTo(7);
        assertThat(node.leaderId()).isEmpty();
    }

    @Test
    void a_candidate_steps_down_when_it_hears_from_a_leader_of_its_own_term() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();

        node.onAppendEntries(4, term);

        assertThat(node.role()).isEqualTo(Role.FOLLOWER);
        assertThat(node.leaderId()).hasValue(4);
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

        assertThat(witness.nextDeadlineNanos()).isEqualTo(Long.MAX_VALUE);

        clock.advance(Duration.ofMinutes(10));
        witness.runOnce(0L);

        assertThat(witness.role()).isEqualTo(Role.FOLLOWER);
        assertThat(witness.electionsStarted()).isZero();
    }

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

    // ------------------------------------------------------------------ pre-vote

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

    private static List<Peer> peers(int... ids) {
        List<Peer> peers = new ArrayList<>();
        for (int id : ids) {
            peers.add(Peer.application(id, "sim", 7000 + id));
        }
        return peers;
    }
}
