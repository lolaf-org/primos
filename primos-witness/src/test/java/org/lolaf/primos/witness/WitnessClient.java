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
package org.lolaf.primos.witness;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.lolaf.primos.wire.DropCounters;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageDecoder;
import org.lolaf.primos.wire.MessageEncoder;
import org.lolaf.primos.wire.MessageHandler;
import org.lolaf.primos.wire.WireFormat;

/**
 * One node of one cluster, talking to a witness server over a real socket.
 *
 * <p>Real datagrams rather than direct handler calls, on purpose: the routing step, the peek at
 * the application name and the per-application authentication only exist on the socket path, so a
 * test that skipped it would be testing something the server does not do.
 */
final class WitnessClient implements AutoCloseable {

    /** Loopback only, so a test never puts traffic on a real network. */
    private static final String LOOPBACK = "127.0.0.1";

    private final String application;
    private final int nodeId;
    private final DatagramChannel channel;
    private final InetSocketAddress witness;
    private final MessageEncoder encoder;
    private final MessageDecoder decoder;
    private final ByteBuffer send = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
    private final ByteBuffer receive = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES + 1);

    WitnessClient(String application, int nodeId, String secret, int witnessId,
                  InetSocketAddress witness) {
        this(application, nodeId, secret, null, witnessId, witness);
    }

    WitnessClient(String application, int nodeId, String secret, String previousSecret,
                  int witnessId, InetSocketAddress witness) {
        this.application = application;
        this.nodeId = nodeId;
        this.witness = witness;
        MessageAuthenticator authenticator = secret == null
                ? MessageAuthenticator.disabled()
                : MessageAuthenticator.hmacSha256(secret.getBytes(StandardCharsets.UTF_8),
                        previousSecret == null ? null : previousSecret.getBytes(StandardCharsets.UTF_8));
        this.encoder = new MessageEncoder(application, nodeId, authenticator);
        this.decoder = new MessageDecoder(application, authenticator,
                id -> id == witnessId, new DropCounters());
        try {
            this.channel = DatagramChannel.open();
            this.channel.configureBlocking(false);
            this.channel.bind(new InetSocketAddress(LOOPBACK, 0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Asks for a vote and returns whether it was granted, driving the server until it answers.
     *
     * @return null if the witness never replied, which is what a dropped datagram looks like
     */
    Boolean requestVote(WitnessServer server, long term) {
        Reply reply = exchange(server, encoder.requestVote(send, term, false));
        return reply == null ? null : reply.granted;
    }

    /** Tells the witness this node is leading, which is what stops it granting pre-votes. */
    void heartbeat(WitnessServer server, long term) {
        exchange(server, encoder.appendEntries(send, term));
    }

    /** Sends without waiting for anything, for traffic that is expected to be dropped. */
    void fireAndForget(WitnessServer server, long term) {
        write(encoder.requestVote(send, term, false));
        server.runOnce(Duration.ofMillis(20).toNanos());
        server.runOnce(0L);
    }

    /**
     * Sends one message and waits for the answer to <em>it</em>.
     *
     * <p>Anything else that arrives meanwhile is ignored rather than mistaken for the answer. The
     * witness probes every voter it has learned about, so a probe landing between the request and
     * its reply is entirely normal — and reading one as a refused vote is exactly the kind of
     * quietly-passing test this suite is meant not to have.
     */
    private Reply exchange(WitnessServer server, ByteBuffer datagram) {
        write(datagram);
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            server.runOnce(Duration.ofMillis(5).toNanos());
            Reply reply = tryReceive();
            if (reply != null && reply.answered) {
                return reply;
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

    /** Whatever the witness answered. */
    static final class Reply implements MessageHandler {

        private long term;
        private boolean granted;
        private boolean answered;
        private boolean handedOver;
        private boolean leaving;
        private boolean timings;

        @Override
        public void onRequestVoteReply(int senderId, long term, boolean granted, boolean preVote) {
            this.term = term;
            this.granted = granted;
            this.answered = true;
        }

        @Override
        public void onRequestVote(int senderId, long term, boolean preVote) {
        }

        @Override
        public void onAppendEntries(int senderId, long term) {
        }

        @Override
        public void onAppendEntriesReply(int senderId, long term, boolean success) {
            this.term = term;
            this.answered = true;
        }

        /** The witness probing this node, which is traffic in its own right rather than an answer. */
        @Override
        public void onPing(int senderId, long term, long sequence) {
        }

        @Override
        public void onPingReply(int senderId, long term, long sequence) {
            this.term = term;
            this.answered = true;
        }

        /** A handover, which a witness must never send. Recorded so a test can prove it did not. */
        @Override
        public void onTimeoutNow(int senderId, long term) {
            this.handedOver = true;
        }

        /** The witness announcing it is going away, which a cluster wants to hear promptly. */
        @Override
        public void onLeaving(int senderId, long term) {
            this.leaving = true;
        }

        @Override
        public void onTimings(int senderId, long term, int heartbeatMillis, int electionMinMillis,
                              int electionMaxMillis, int probeMillis, int failureThreshold) {
            this.timings = true;
        }
    }
}
