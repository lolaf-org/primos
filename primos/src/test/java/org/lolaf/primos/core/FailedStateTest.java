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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Role;
import org.lolaf.primos.store.StateStoreException;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.DatagramHandler;
import org.lolaf.primos.transport.SimulatedNetwork;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.WireFormat;

/**
 * A node that dies must look dead.
 *
 * <p>An uncaught exception that killed the loop while leaving {@code role()} at {@code LEADER}
 * would be the same split brain as a partitioned leader that never stands down, arrived at through
 * a broken disk instead of a broken network.
 */
class FailedStateTest {

    private final VirtualClock clock = new VirtualClock();
    private final SimulatedNetwork network = new SimulatedNetwork(1L, clock);
    private final FailingStateStore store = new FailingStateStore();

    @Test
    void a_store_that_breaks_takes_the_node_to_failed() {
        ElectionNode node = startedNode();
        store.breakWith(FailingStateStore.brokenDisk());

        requestVoteFrom(2, 9);
        node.runOnce(0L);

        assertThat(node.role()).isEqualTo(Role.FAILED);
        assertThat(node.isFailed()).isTrue();
        assertThat(node.failureCause()).isInstanceOf(StateStoreException.class);
    }

    @Test
    void a_leader_whose_disk_dies_stops_being_a_leader() {
        ElectionNode node = electedLeader();
        assertThat(node.role()).isEqualTo(Role.LEADER);

        store.breakWith(FailingStateStore.brokenDisk());
        appendEntriesFrom(2, node.currentTerm() + 5);
        node.runOnce(0L);

        assertThat(node.role())
                .as("the application must not go on believing it is in charge")
                .isEqualTo(Role.FAILED);
        assertThat(node.roleAndTerm().isLeader()).isFalse();
    }

    @Test
    void a_failed_node_never_leads_again_however_long_it_runs() {
        ElectionNode node = failedNode();

        for (int i = 0; i < 100; i++) {
            clock.advance(Duration.ofSeconds(1));
            node.runOnce(0L);
        }

        assertThat(node.role()).isEqualTo(Role.FAILED);
    }

    @Test
    void a_failed_node_stops_doing_anything() {
        ElectionNode node = failedNode();
        long electionsBefore = node.electionsStarted();
        long preVotesBefore = node.preVotesStarted();

        clock.advance(Duration.ofMinutes(5));
        node.runOnce(0L);

        assertThat(node.electionsStarted()).isEqualTo(electionsBefore);
        assertThat(node.preVotesStarted()).isEqualTo(preVotesBefore);
        assertThat(node.nextDeadlineNanos())
                .as("nothing left to wait for")
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void a_transport_that_throws_fails_the_node_rather_than_the_caller() {
        ElectionNode node = nodeWithTransport(new ThrowingTransport(
                new IllegalStateException("socket exploded")));

        assertThatCode(() -> node.runOnce(0L))
                .as("the owning loop learns from the role, not from a stack trace")
                .doesNotThrowAnyException();
        assertThat(node.role()).isEqualTo(Role.FAILED);
        assertThat(node.failureCause()).hasMessage("socket exploded");
    }

    @Test
    void an_error_is_still_rethrown_but_only_after_the_node_is_safe() {
        ElectionNode node = nodeWithTransport(new ThrowingTransport(new StackOverflowError()));

        assertThatThrownBy(() -> node.runOnce(0L)).isInstanceOf(StackOverflowError.class);

        assertThat(node.role())
                .as("marked failed before anything propagates, so role() is never stale")
                .isEqualTo(Role.FAILED);
    }

    @Test
    void the_failed_role_is_not_a_leading_role() {
        assertThat(Role.FAILED.isLeader()).isFalse();
        assertThat(Role.FOLLOWER.isLeader()).isFalse();
        assertThat(Role.CANDIDATE.isLeader()).isFalse();
        assertThat(Role.LEADER.isLeader()).isTrue();
    }

    private ElectionNode failedNode() {
        ElectionNode node = startedNode();
        store.breakWith(FailingStateStore.brokenDisk());
        requestVoteFrom(2, 9);
        node.runOnce(0L);
        assertThat(node.role()).isEqualTo(Role.FAILED);
        return node;
    }

    /** Drives a node of three all the way to leadership over the simulated wire. */
    private ElectionNode electedLeader() {
        ElectionNode node = startedNode();

        clock.advance(Duration.ofSeconds(1));
        node.runOnce(0L);

        requestVoteReplyFrom(2, 1, true, true);
        node.runOnce(0L);
        assertThat(node.role()).isEqualTo(Role.CANDIDATE);

        requestVoteReplyFrom(2, node.currentTerm(), true, false);
        node.runOnce(0L);
        return node;
    }

    private void requestVoteFrom(int from, long term) {
        deliver(from, (encoder, buffer) -> encoder.requestVote(buffer, term, false));
    }

    private void requestVoteReplyFrom(int from, long term, boolean granted, boolean preVote) {
        deliver(from, (encoder, buffer) ->
                encoder.requestVoteReply(buffer, term, granted, preVote));
    }

    private void appendEntriesFrom(int from, long term) {
        deliver(from, (encoder, buffer) -> encoder.appendEntries(buffer, term));
    }

    /**
     * Sends a real datagram from a peer, so the failure arrives the way it would in production:
     * through the loop, not by calling a handler directly.
     */
    private void deliver(int from, java.util.function.BiFunction<MessageEncoder, ByteBuffer, ByteBuffer> message) {
        MessageEncoder encoder = new MessageEncoder("t", from, MessageAuthenticator.disabled());
        ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
        network.transportFor(from).send(1, message.apply(encoder, buffer));
    }

    private ElectionNode startedNode() {
        return nodeWithTransport(network.transportFor(1));
    }

    private ElectionNode nodeWithTransport(Transport transport) {
        List<Peer> peers = new ArrayList<>();
        peers.add(Peer.application(2, "sim", 7002));
        peers.add(Peer.application(3, "sim", 7003));
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("t").nodeId(1).peers(peers).build();
        ElectionNode node = new ElectionNode(config, store, transport, clock,
                new Random(1L), MessageAuthenticator.disabled());
        node.start();
        return node;
    }

    /** A transport whose poll always blows up. */
    private static final class ThrowingTransport implements Transport {

        private final Throwable failure;

        private long sendsDropped;

        private ThrowingTransport(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void send(int peerId, ByteBuffer datagram) {
            // Nothing goes anywhere, and the SPI says a transport must be able to say so.
            sendsDropped++;
        }

        @Override
        public long sendsDropped() {
            return sendsDropped;
        }

        @Override
        public int poll(long timeoutNanos, DatagramHandler handler) {
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw (RuntimeException) failure;
        }

        @Override
        public void wakeup() {
        }

        @Override
        public void close() {
        }
    }
}
