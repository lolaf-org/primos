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
package org.lolaf.primos.witness.boot;

import org.lolaf.primos.PrimosStatsListener;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageDecoder;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.MessageHandler;
import org.lolaf.primos.wire.WireFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * A cluster node, over a real socket, for a witness that is running on its own thread.
 *
 * <p>Deliberately not the witness module's own client, which drives {@code runOnce} between sends:
 * here the loop is Spring's to run, so the whole point is to wait for datagrams that arrive without
 * this thread turning any handle. That is also what makes these tests the production path — nothing
 * here reaches into the server.
 */
final class WitnessTestClient implements AutoCloseable {

    private static final String LOOPBACK = "127.0.0.1";

    private final InetSocketAddress witness;
    private final DatagramChannel channel;
    private final MessageEncoder encoder;
    private final MessageDecoder decoder;
    private final ByteBuffer send = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
    private final ByteBuffer receive = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES + 1);

    WitnessTestClient(String application, int nodeId, String secret, int witnessId,
                      InetSocketAddress witness) {
        this.witness = witness;
        MessageAuthenticator authenticator = MessageAuthenticator.hmacSha256(
                secret.getBytes(StandardCharsets.UTF_8), null);
        this.encoder = new MessageEncoder(application, nodeId, authenticator);
        this.decoder = new MessageDecoder(application, authenticator, id -> id == witnessId,
                PrimosStatsListener.NONE);
        try {
            this.channel = DatagramChannel.open();
            this.channel.configureBlocking(false);
            this.channel.bind(new InetSocketAddress(LOOPBACK, 0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Asks for a vote and waits for the answer to <em>that</em>.
     *
     * <p>Anything else arriving meanwhile is ignored rather than mistaken for the reply: a witness
     * probes every voter it has learned about, so a probe landing between the request and its
     * answer is ordinary, and reading one as a refused vote is how a broken reply path passes a
     * test.
     */
    Boolean requestVote(long term, Duration timeout) {
        write(encoder.requestVote(send, term, false));
        Reply reply = awaitReply(timeout, received -> received.answered);
        return reply == null ? null : reply.granted;
    }

    /** Waits for the witness to say it is going away, which is what a clean shutdown sends. */
    boolean awaitDeparture(Duration timeout) {
        return awaitReply(timeout, received -> received.leaving) != null;
    }

    private Reply awaitReply(Duration timeout, java.util.function.Predicate<Reply> wanted) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Reply reply = tryReceive();
            if (reply != null && wanted.test(reply)) {
                return reply;
            }
            if (reply == null) {
                // Nothing had arrived. The server is on its own thread, so the only thing to do is
                // let it get on with it.
                Thread.onSpinWait();
            }
        }
        return null;
    }

    private void write(ByteBuffer datagram) {
        try {
            channel.send(datagram, witness);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Reply tryReceive() {
        try {
            receive.clear();
            SocketAddress from = channel.receive(receive);
            if (from == null) {
                return null;
            }
            receive.flip();
            Reply reply = new Reply();
            return decoder.decode(receive, reply) ? reply : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Whatever the witness said. */
    static final class Reply implements MessageHandler {

        private boolean granted;
        private boolean answered;
        private boolean leaving;

        @Override
        public void onRequestVoteReply(int senderId, long term, boolean granted, boolean preVote) {
            this.granted = granted;
            this.answered = true;
        }

        @Override
        public void onLeaving(int senderId, long term) {
            this.leaving = true;
        }

        @Override
        public void onRequestVote(int senderId, long term, boolean preVote) {
        }

        @Override
        public void onAppendEntries(int senderId, long term) {
        }

        @Override
        public void onAppendEntriesReply(int senderId, long term, boolean success) {
        }

        @Override
        public void onPing(int senderId, long term, long sequence) {
        }

        @Override
        public void onPingReply(int senderId, long term, long sequence) {
        }

        @Override
        public void onTimeoutNow(int senderId, long term) {
        }

        @Override
        public void onNodeConfig(int senderId, long term, int heartbeatMillis,
                                int electionMinMillis, int electionMaxMillis, int probeMillis,
                                int failureThreshold) {
        }
    }
}
