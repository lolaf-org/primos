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
package org.lolaf.primos.wire;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Writes the six messages into a caller-owned buffer.
 *
 * <p>Allocation-free: the application name is padded once at construction, and every message is
 * written into a buffer the node reuses for the life of the process. The buffer comes back flipped
 * and ready to send.
 *
 * <p>The sender id is fixed at construction, because a node only ever sends as itself.
 *
 * <p>Not thread-safe, and not meant to be: one encoder belongs to one node thread.
 */
public final class MessageEncoder {

    private final byte[] applicationName;
    private final int senderId;
    private final MessageAuthenticator authenticator;

    public MessageEncoder(String applicationName, int senderId, MessageAuthenticator authenticator) {
        this.applicationName = padded(applicationName);
        this.senderId = senderId;
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
    }

    /**
     * Pads the name into the fixed 32-byte field. Names are validated on the way into
     * {@link org.lolaf.primos.ClusterConfig}, so anything reaching here already fits.
     */
    static byte[] padded(String applicationName) {
        Objects.requireNonNull(applicationName, "applicationName");
        byte[] raw = applicationName.getBytes(StandardCharsets.UTF_8);
        if (raw.length == 0 || raw.length > WireFormat.APPLICATION_BYTES) {
            throw new IllegalArgumentException(
                    "applicationName must be 1 to " + WireFormat.APPLICATION_BYTES
                            + " bytes, but '" + applicationName + "' is " + raw.length);
        }
        byte[] field = new byte[WireFormat.APPLICATION_BYTES];
        System.arraycopy(raw, 0, field, 0, raw.length);
        return field;
    }

    /**
     * A vote request, or with {@code preVote} set, a question about whether a vote would be given.
     */
    public ByteBuffer requestVote(ByteBuffer out, long term, boolean preVote) {
        header(out, MessageType.REQUEST_VOTE, preVote ? WireFormat.FLAG_PRE_VOTE : 0, term);
        return finish(out);
    }

    /**
     * A vote reply, echoing whether this was a pre-vote so the two rounds cannot be confused.
     */
    public ByteBuffer requestVoteReply(ByteBuffer out, long term, boolean granted, boolean preVote) {
        header(out, MessageType.REQUEST_VOTE_REPLY, preVote ? WireFormat.FLAG_PRE_VOTE : 0, term);
        out.put(granted ? (byte) 1 : (byte) 0);
        return finish(out);
    }

    /**
     * The heartbeat. No payload: the term and the leader's id are both in the header.
     */
    public ByteBuffer appendEntries(ByteBuffer out, long term) {
        header(out, MessageType.APPEND_ENTRIES, 0, term);
        return finish(out);
    }

    /**
     * A heartbeat acknowledgement.
     */
    public ByteBuffer appendEntriesReply(ByteBuffer out, long term, boolean success) {
        header(out, MessageType.APPEND_ENTRIES_REPLY, 0, term);
        out.put(success ? (byte) 1 : (byte) 0);
        return finish(out);
    }

    /**
     * Tells one follower to campaign now, because this leader is going away.
     *
     * <p>Carries nothing but the header: the target needs to know who is handing over and in which
     * term, and both of those are already there.
     */
    public ByteBuffer timeoutNow(ByteBuffer out, long term) {
        header(out, MessageType.TIMEOUT_NOW, 0, term);
        return finish(out);
    }

    /**
     * Tells a peer this node is shutting down, so it need not wait out a probe timeout.
     *
     * <p>Nothing but the header, because it asserts nothing beyond its own sender's departure.
     */
    public ByteBuffer leaving(ByteBuffer out, long term) {
        header(out, MessageType.LEAVING, 0, term);
        return finish(out);
    }

    /**
     * A reachability probe.
     */
    public ByteBuffer ping(ByteBuffer out, long term, long sequence) {
        header(out, MessageType.PING, 0, term);
        out.putLong(sequence);
        return finish(out);
    }

    /**
     * A probe reply, echoing the sequence number so the round trip can be measured.
     */
    public ByteBuffer pingReply(ByteBuffer out, long term, long sequence) {
        header(out, MessageType.PING_REPLY, 0, term);
        out.putLong(sequence);
        return finish(out);
    }

    /**
     * States this node's own configuration — the intervals it is running.
     *
     * <p>Milliseconds as {@code u32} rather than nanoseconds as {@code u64}: milliseconds are the
     * unit every one of these is configured in, and 32 bits carries seven weeks of them.
     */
    public ByteBuffer nodeConfig(ByteBuffer out, long term, int heartbeatMillis,
                                int electionMinMillis, int electionMaxMillis,
                                int probeMillis, int failureThreshold) {
        header(out, MessageType.NODE_CONFIG, 0, term);
        out.putInt(heartbeatMillis);
        out.putInt(electionMinMillis);
        out.putInt(electionMaxMillis);
        out.putInt(probeMillis);
        out.putInt(failureThreshold);
        return finish(out);
    }

    /**
     * How many bytes a message of this type occupies, tag included.
     */
    public int sizeOf(MessageType type) {
        return type.messageBytes() + authenticator.tagBytes();
    }

    private void header(ByteBuffer out, MessageType type, int flags, long term) {
        int needed = sizeOf(type);
        if (out.capacity() < needed) {
            throw new IllegalArgumentException(
                    "buffer holds " + out.capacity() + " bytes but " + type + " needs " + needed);
        }
        out.clear();
        out.put(WireFormat.MAGIC);
        out.put(WireFormat.VERSION);
        out.put((byte) type.code());
        out.putShort((short) flags);
        out.put(applicationName);
        out.putLong(term);
        out.putInt(senderId);
    }

    private ByteBuffer finish(ByteBuffer out) {
        authenticator.appendTag(out);
        out.flip();
        return out;
    }
}
