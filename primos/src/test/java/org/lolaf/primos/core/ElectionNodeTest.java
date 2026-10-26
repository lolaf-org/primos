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
import org.lolaf.primos.*;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One node in isolation, for the two properties that are easiest to prove by reaching straight
 * into it rather than by waiting for a cluster to misbehave.
 */
class ElectionNodeTest {

    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network = new SimulatedNetwork(1L, clock);
    private final RecordingStats recorded = new RecordingStats();
    private final StateStore store = new MemoryStateStore();

    private static List<Peer> peers(int... ids) {
        List<Peer> peers = new ArrayList<>();
        for (int id : ids) {
            peers.add(Peer.application(id, "sim", 7000 + id));
        }
        return peers;
    }

    private static int countNodeConfigs(CapturedReply seen) {
        int count = 0;
        for (String message : seen.messages()) {
            if (message.startsWith("nodeConfig(")) {
                count++;
            }
        }
        return count;
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

        node.onAppendEntries(3, 6, 0, 0);

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
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);
        witness.start();

        // The witness server learns voters between turns of its own loop, so a heartbeat can
        // arrive before learnPeer has been called for its sender. Every other node answers only
        // configured senders, so this is the one place the peer can be missing.
        witness.onAppendEntries(7, 1, 0, 0);

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

        node.onAppendEntries(3, 4, 0, 0);

        assertThat(node.currentTerm()).isEqualTo(7);
        assertThat(node.leaderOptional()).isEmpty();
    }

    @Test
    void a_candidate_steps_down_when_it_hears_from_a_leader_of_its_own_term() {
        ElectionNode node = campaigningNodeOfFive();
        long term = node.currentTerm();

        node.onAppendEntries(4, term, 0, 0);

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
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);
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

    /**
     * Not a tidiness check, which is why it is worth a test and worth its place on the loop path.
     *
     * <p>{@link ElectionNode#start()} is what reads {@code currentTerm} and {@code votedFor} back
     * from the store. A turn of the loop taken without it would run on term 0 with no vote — a node
     * that has forgotten what it promised in a previous life — and would then time out, campaign
     * and vote for itself in a term it may already have voted in. That is the double-vote bug,
     * reached by calling two methods in the wrong order.
     *
     * <p>{@code ElectionNode} is public only because {@code primos-witness} is a separate artifact,
     * so the caller who could get this wrong is a real one. Both callers in this repository start
     * first; nothing but this says what happens to a third.
     */
    @Test
    void a_turn_of_the_loop_before_the_store_has_been_read_is_refused() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(1)
                .peers(peers(2, 3))
                .build();
        ElectionNode node = new ElectionNode(config, store, network.transportFor(1), clock,
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);

        assertThatThrownBy(() -> node.runOnce(0))
                .as("silently running on term 0 would be a node that had forgotten its vote")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has not been started");

        assertThat(store.load().currentTerm())
                .as("and nothing was written on the way to refusing")
                .isZero();
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
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);
        witness.start();

        witness.onRequestVote(1, 3, false);

        assertThat(witness.votedFor()).isEqualTo(1);
        assertThat(witness.currentTerm()).isEqualTo(3);
    }

    @Test
    void a_pre_vote_is_refused_while_a_leader_is_being_heard_from() {
        ElectionNode node = startedNode();
        node.onAppendEntries(2, 1, 0, 0);

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
        node.onAppendEntries(2, 1, 0, 0);
        clock.advance(Duration.ofSeconds(1));

        node.onRequestVote(3, 2, true);

        assertThat(new CapturedReply("t").drain(network.transportFor(3)).granted()).isTrue();
    }

    @Test
    void a_pre_vote_is_refused_for_an_election_timeout_after_granting_a_vote() {
        ElectionNode node = startedNode();
        clock.advance(Duration.ofSeconds(1));
        node.onRequestVote(2, 1, false);

        node.onRequestVote(3, 2, true);

        assertThat(new CapturedReply("t").drain(network.transportFor(3)).granted())
                .as("node 2 may lead from its election on, and its lease counts on this refusal")
                .isFalse();
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

    // ------------------------------------------------------------------ timing advertisements

    @Test
    void a_single_node_cluster_skips_straight_through_its_own_pre_vote() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t").nodeId(1).build();
        ElectionNode node = new ElectionNode(config, store, network.transportFor(1), clock,
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);
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

    /**
     * Logged and counted on the transition, not on every advert. A peer restates its intervals
     * every thirty seconds for as long as it runs, so counting datagrams would turn one
     * misconfiguration into an ever-growing number and a log line every half minute.
     */
    @Test
    void a_peer_running_different_intervals_is_reported_once_not_on_every_advert() {
        ElectionNode node = startedNode();

        advertise(2, node, 200, 1500, 3000, 2000, 3);
        assertThat(recorded.timingMismatches.get()).isEqualTo(1);

        advertise(2, node, 200, 1500, 3000, 2000, 3);
        advertise(2, node, 200, 1500, 3000, 2000, 3);

        assertThat(recorded.timingMismatches.get())
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
        assertThat(recorded.timingMismatches.get()).isEqualTo(1);

        advertise(2, node, 50, 300, 600, 1000, 3);
        assertThat(recorded.timingMismatches.get())
                .as("agreeing again is not another mismatch")
                .isEqualTo(1);

        advertise(2, node, 200, 1500, 3000, 2000, 3);
        assertThat(recorded.timingMismatches.get())
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
        assertThat(recorded.timingMismatches.get()).isEqualTo(1);
    }

    @Test
    void a_peer_running_the_same_intervals_is_never_mentioned() {
        ElectionNode node = startedNode();

        advertise(2, node, 50, 300, 600, 1000, 3);

        assertThat(recorded.timingMismatches.get())
                .as("silence is the correct output for a cluster that agrees")
                .isZero();
    }

    /**
     * One statement every thirty seconds is the right steady state and the wrong bootstrap: this is
     * UDP, and the case where the first is dropped is the case that most needs it — a witness taking
     * its configuration from the cluster cannot serve one it has never heard state itself.
     */
    @Test
    void a_node_restates_itself_at_probe_cadence_while_a_peer_has_not_answered() {
        ElectionNode node = startedNode();

        int statements = countNodeConfigsOverTenSeconds(node);

        assertThat(statements)
                .as("nobody has answered, so a lost statement must not cost an advertisement"
                        + " interval of silence")
                .isGreaterThan(5);
    }

    /**
     * The negative pair, and without it the test above passes on a node that simply states itself
     * constantly — which would be the same bug wearing the fix's clothes.
     */
    @Test
    void it_settles_to_the_advertisement_interval_once_every_peer_has_answered() {
        ElectionNode node = startedNode(peers(2));
        // A real node at 2, so its probes are answered and it becomes reachable.
        ClusterConfig peerConfig = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(2)
                .peer(Peer.application(1, "h", 1))
                .build();
        ElectionNode peer = new ElectionNode(peerConfig, new MemoryStateStore(),
                network.transportFor(2), clock, new Random(2L), MessageAuthenticator.disabled());
        peer.start();

        // Let them find each other first, then measure the settled cadence.
        for (int i = 0; i < 40; i++) {
            clock.advance(Duration.ofMillis(250));
            node.runOnce(0L);
            peer.runOnce(0L);
        }
        CapturedReply seen = new CapturedReply("t");
        int before = countNodeConfigs(seen.drain(network.transportFor(2)));
        for (int i = 0; i < 40; i++) {
            clock.advance(Duration.ofMillis(250));
            node.runOnce(0L);
            peer.runOnce(0L);
        }

        int statements = countNodeConfigs(seen.drain(network.transportFor(2))) - before;
        assertThat(statements)
                .as("a cluster whose peers all answer says this once per advertisement interval,"
                        + " not once per probe")
                .isLessThanOrEqualTo(1);
    }

    private int countNodeConfigsOverTenSeconds(ElectionNode node) {
        CapturedReply seen = new CapturedReply("t");
        for (int i = 0; i < 40; i++) {
            clock.advance(Duration.ofMillis(250));
            node.runOnce(0L);
            seen.drain(network.transportFor(2));
        }
        return countNodeConfigs(seen);
    }

    /**
     * The pair to the tests above: a witness told to follow reports nothing, because there is
     * nothing to report — the difference it sees is the value it is about to be rebuilt with, so
     * an ERROR on every retune would describe a state lasting one turn as a fault.
     */
    @Test
    void a_witness_that_follows_its_cluster_reports_no_mismatch() {
        ElectionNode witness = startedWitness(true);

        advertise(2, witness, 200, 1500, 3000, 2000, 3);

        assertThat(recorded.timingMismatches.get())
                .as("following is not disagreeing")
                .isZero();
        assertThat(witness.observedTimings())
                .as("it still records what was stated; suppressing the report is not ignoring it")
                .isNotNull();
        assertThat(witness.observedTimings().heartbeatInterval().toMillis()).isEqualTo(200);
    }

    /**
     * Without this the test above would pass on a witness that had simply stopped comparing.
     */
    @Test
    void a_witness_that_does_not_follow_still_reports_a_mismatch() {
        ElectionNode witness = startedWitness(false);

        advertise(2, witness, 200, 1500, 3000, 2000, 3);

        assertThat(recorded.timingMismatches.get()).isEqualTo(1);
    }

    // ------------------------------------------------------------ learned peers

    @Test
    void a_witness_forgets_a_learned_voter_that_goes_quiet() {
        ElectionNode witness = startedLearningWitness();
        witness.learnPeer(Peer.application(7, "h", 1));
        assertThat(witness.trackedPeerIds()).contains(7);

        // Far past sixty probe intervals with nothing arriving from it.
        clock.advance(Duration.ofMinutes(5));
        witness.runOnce(0L);

        assertThat(witness.trackedPeerIds())
                .as("otherwise a tenant whose node ids roll leaves a tracked peer and a probe every"
                        + " interval behind for every id it has ever used")
                .doesNotContain(7);
        assertThat(witness.peersForgotten()).isEqualTo(1);
    }

    /**
     * The pair to the test above, and the one that matters more: a peer that is merely <em>down</em>
     * is exactly the peer a witness should keep watching. Down is three missed probes; gone is a
     * minute of silence, and confusing them loses the node during a garbage collection.
     */
    @Test
    void a_voter_that_is_only_down_is_kept() {
        ElectionNode witness = startedLearningWitness();
        witness.learnPeer(Peer.application(7, "h", 1));

        // Long enough to miss every probe and be reported down, nowhere near long enough to be gone.
        clock.advance(Duration.ofSeconds(10));
        witness.runOnce(0L);

        assertThat(witness.trackedPeerIds()).contains(7);
        assertThat(witness.isReachable(7))
                .as("down, and still tracked; those are different questions")
                .isFalse();
        assertThat(witness.peersForgotten()).isZero();
    }

    @Test
    void a_forgotten_voter_is_learned_again_the_moment_it_speaks() {
        ElectionNode witness = startedLearningWitness();
        witness.learnPeer(Peer.application(7, "h", 1));
        clock.advance(Duration.ofMinutes(5));
        witness.runOnce(0L);
        assertThat(witness.trackedPeerIds()).doesNotContain(7);

        assertThat(witness.learnPeer(Peer.application(7, "h", 1)))
                .as("forgetting is cheap precisely because this costs nothing")
                .isTrue();
        assertThat(witness.trackedPeerIds()).contains(7);
    }

    @Test
    void a_departure_announcement_does_not_forget_a_voter() {
        ElectionNode witness = startedLearningWitness();
        witness.learnPeer(Peer.application(7, "h", 1));

        witness.onLeaving(7, 0);
        witness.runOnce(0L);

        assertThat(witness.trackedPeerIds())
                .as("a departure is a hint that can be wrong, and eviction reachable by one"
                        + " datagram is a door worth not opening")
                .contains(7);
        assertThat(witness.peersForgotten()).isZero();
    }

    @Test
    void a_node_with_a_configured_peer_set_never_forgets_one() {
        ElectionNode node = startedNode();

        clock.advance(Duration.ofHours(1));
        node.runOnce(0L);

        assertThat(node.trackedPeerIds())
                .as("a configured peer set is the quorum; losing one silently would be a way to"
                        + " shrink it")
                .contains(2, 3);
        assertThat(node.peersForgotten()).isZero();
    }

    @Test
    void the_learned_set_has_a_ceiling_and_the_quietest_makes_room() {
        ElectionNode witness = startedLearningWitness();
        for (int id = 1; id <= 300; id++) {
            if (id != witness.config().nodeId()) {
                witness.learnPeer(Peer.application(id, "h", id));
            }
        }

        assertThat(witness.trackedPeerIds())
                .as("ageing bounds the set by legitimate traffic; this bounds it against a"
                        + " keyholder cycling sender ids faster than that")
                .hasSizeLessThanOrEqualTo(256);
        assertThat(witness.trackedPeerIds())
                .as("and it is the quietest that went, so the newest arrivals are still tracked")
                .contains(300);
    }

    private ElectionNode startedLearningWitness() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(1)
                .asWitness()
                .learnedMembership()
                .build();
        ElectionNode node = new ElectionNode(config, store, network.transportFor(1), clock,
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);
        node.start();
        return node;
    }

    private ElectionNode startedWitness(boolean follows) {
        ClusterConfig.ClusterConfigBuilder config = ClusterConfig.builder()
                .applicationName("t")
                .nodeId(1)
                .asWitness()
                .peer(Peer.application(2, "h", 1));
        if (follows) {
            config.followsStatedTimings();
        }
        ElectionNode node = new ElectionNode(config.build(), store, network.transportFor(1), clock,
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);
        node.start();
        return node;
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
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() {
        }, recorded);
        node.start();
        return node;
    }
}
