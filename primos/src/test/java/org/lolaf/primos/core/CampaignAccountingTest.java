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
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.RecordingStats;
import org.lolaf.primos.Role;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.SimulatedNetwork;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.WireFormat;

/**
 * Every campaign is reported exactly once, and its duration covers that campaign and no earlier one.
 */
class CampaignAccountingTest {

    private static final int PEER = 2;
    private static final long TERM = 5;
    private static final Duration PAST_ANY_TIMER = Duration.ofSeconds(1);
    private static final Duration A_WEEK = Duration.ofDays(7);

    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network = new SimulatedNetwork(1L, clock);
    private final RecordingStats recorded = new RecordingStats();
    private final StateStore store = new MemoryStateStore();
    private ElectionNode node;

    @BeforeEach
    void startNode() {
        network.delayRange(0, 0);
        store.save(TERM, PersistentState.NO_VOTE);
        node = new ElectionNode(ClusterConfig.builder().applicationName("t").nodeId(1)
                .peers(List.of(Peer.application(PEER, "sim", 7002), Peer.application(3, "sim", 7003))).build(),
                store, network.transportFor(1), clock, new Random(1L), MessageAuthenticator.disabled(),
                new PrimosListener() { }, recorded);
        node.start();
    }

    @Test
    void a_pre_vote_interrupted_by_its_leader_is_reported_as_lost() {
        startPreVote();

        heartbeat();

        assertThat(recorded.electionsLost.get()).isEqualTo(1);
        assertThat(recorded.campaignsLost).hasSize(1);
    }

    @Test
    void the_next_campaign_does_not_inherit_an_interrupted_one_s_start() {
        startPreVote();
        heartbeat();

        clock.advance(A_WEEK);
        startPreVote();
        win();

        assertThat(recorded.campaignsWon).hasSize(1);
        assertThat(Duration.ofNanos(recorded.campaignsWon.get(0))).isLessThan(PAST_ANY_TIMER);
    }

    @Test
    void withdrawing_during_a_pre_vote_is_reported_as_lost_and_forgotten() {
        startPreVote();

        node.setCandidate(false);

        assertThat(recorded.electionsLost.get()).isEqualTo(1);

        node.setCandidate(true);
        clock.advance(A_WEEK);
        startPreVote();
        win();
        assertThat(Duration.ofNanos(recorded.campaignsWon.get(0))).isLessThan(PAST_ANY_TIMER);
    }

    @Test
    void a_heartbeat_with_no_campaign_running_reports_nothing() {
        heartbeat();
        heartbeat();

        assertThat(recorded.electionsLost.get()).isZero();
        assertThat(recorded.electionsWon.get()).isZero();
    }

    @Test
    void a_campaign_that_retried_its_pre_vote_counts_from_the_first_one() {
        startPreVote();
        startPreVote();
        win();

        assertThat(Duration.ofNanos(recorded.campaignsWon.get(0))).isGreaterThanOrEqualTo(PAST_ANY_TIMER);
        assertThat(recorded.electionsLost.get()).isZero();
    }

    @Test
    void a_handover_that_wins_is_reported_once_and_is_short() {
        heartbeat();

        deliver((encoder, buffer) -> encoder.timeoutNow(buffer, TERM));
        node.runOnce(0L);
        assertThat(node.role()).isEqualTo(Role.CANDIDATE);
        clock.advance(Duration.ofMillis(3));
        deliver((encoder, buffer) -> encoder.requestVoteReply(buffer, node.currentTerm(), true, false));
        node.runOnce(0L);

        assertThat(node.role()).isEqualTo(Role.LEADER);
        assertThat(recorded.electionsWon.get()).isEqualTo(1);
        assertThat(recorded.campaignsWon).containsExactly(Duration.ofMillis(3).toNanos());
    }

    @Test
    void a_handover_that_loses_is_reported_as_lost() {
        heartbeat();
        deliver((encoder, buffer) -> encoder.timeoutNow(buffer, TERM));
        node.runOnce(0L);

        deliver((encoder, buffer) -> encoder.appendEntries(buffer, node.currentTerm() + 1, 0, 0));
        node.runOnce(0L);

        assertThat(recorded.electionsLost.get()).isEqualTo(1);
        assertThat(recorded.electionsWon.get()).isZero();
    }

    private void startPreVote() {
        clock.advance(PAST_ANY_TIMER);
        node.runOnce(0L);
    }

    private void heartbeat() {
        deliver((encoder, buffer) -> encoder.appendEntries(buffer, TERM, 0, 0));
        node.runOnce(0L);
    }

    private void win() {
        long proposed = node.currentTerm() + 1;
        deliver((encoder, buffer) -> encoder.requestVoteReply(buffer, proposed, true, true));
        node.runOnce(0L);
        assertThat(node.role()).isEqualTo(Role.CANDIDATE);
        deliver((encoder, buffer) -> encoder.requestVoteReply(buffer, node.currentTerm(), true, false));
        node.runOnce(0L);
        assertThat(node.role()).isEqualTo(Role.LEADER);
    }

    private void deliver(BiFunction<MessageEncoder, ByteBuffer, ByteBuffer> message) {
        MessageEncoder encoder = new MessageEncoder("t", PEER, MessageAuthenticator.disabled());
        network.transportFor(PEER).send(1, message.apply(encoder, ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES)));
    }
}
