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

import lombok.Getter;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.DropReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.IntPredicate;

/**
 * The receive pipeline: seven ordered checks, then a dispatch.
 *
 * <p>The order is fixed and each step has its own counter. Nothing below step 5 ever sees a byte
 * that has not passed everything above it.
 *
 * <ol>
 *   <li><b>Length</b> — at least a header, at most one unfragmented datagram.</li>
 *   <li><b>Authentication</b> — the tag over the whole datagram. Unconditionally first, because
 *       "verify before parse" is only useful as a rule with no exceptions, even though the
 *       application-name compare below is cheaper than a SHA-256.</li>
 *   <li><b>Magic and version</b> — exact match; an unknown version is dropped, never guessed.</li>
 *   <li><b>Application name</b> — is this datagram even for our cluster.</li>
 *   <li><b>Type and exact size</b> — one comparison kills every truncated or padded datagram.</li>
 *   <li><b>Sender</b> — a configured peer, by id rather than by address.</li>
 *   <li><b>Parse and dispatch</b> — constant offsets into the handler.</li>
 * </ol>
 *
 * <p>Reads are absolute, so the buffer's position is untouched and nothing is copied. Allocation
 * happens only on the diagnostic path, when a mismatched application name is decoded for the log
 * line — which is the rare case, and the case where knowing the name is the whole point.
 *
 * <p>Not thread-safe: one decoder per node thread.
 */
public final class MessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(MessageDecoder.class);

    private final byte[] expectedApplication;
    private final MessageAuthenticator authenticator;
    private final IntPredicate knownSender;
    private final DropCounters drops;

    /**
     * The sender of the last datagram that passed every check, or -1.
     *
     * <p>Exists so a transport can be told where a peer is speaking from, but only once the
     * message has been authenticated — learning an address from an unverified datagram would let
     * one spoofed packet redirect a peer's traffic.
     */
    @Getter
    private int lastSenderId = -1;

    /**
     * @param knownSender whether a sender id belongs to this cluster. A node answers only its
     *                    configured peers; a witness server may accept ids it has not seen before,
     *                    which is why this is a predicate rather than a fixed set.
     */
    public MessageDecoder(String applicationName,
                          MessageAuthenticator authenticator,
                          IntPredicate knownSender,
                          DropCounters drops) {
        this.expectedApplication = MessageEncoder.padded(applicationName);
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
        this.knownSender = Objects.requireNonNull(knownSender, "knownSender");
        this.drops = Objects.requireNonNull(drops, "drops");
    }

    /**
     * The application name a datagram claims, read <em>before</em> anything has been verified.
     *
     * <p>Every other reader in primos verifies the tag first, unconditionally, because "verify
     * before parse" is only useful as a rule with no exceptions. <strong>The witness server cannot
     * follow it.</strong> Its secret is per-application, so it has to know which application a
     * datagram claims to be for before it knows which key to check it with — and that claim is in
     * the datagram's own unverified bytes.
     *
     * <p>That is sound, and the reasons are worth stating rather than rediscovering. What is read
     * here is a fixed-offset, fixed-width field: no allocation sized by attacker input, no length
     * arithmetic, nothing that can be walked off the end. And the claim buys nothing on its own —
     * an attacker may name any application they like and still cannot produce a tag that
     * application's key accepts. The real decoder then re-checks the name after verifying, so
     * routing and acceptance can never disagree.
     *
     * @return the claimed name, or null if the datagram is too short to carry one or the field
     * does not hold a name that could ever have been sent
     */
    public static String peekApplicationName(ByteBuffer datagram) {
        if (datagram.limit() < WireFormat.HEADER_BYTES) {
            return null;
        }
        String name = readApplicationName(datagram);
        if (name.isEmpty()) {
            return null;
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!ClusterConfig.isApplicationNameCharacter(c)) {
                // Not a name any encoder would have produced, so there is nothing to route it to.
                // Returning it would put arbitrary bytes into a log line and a map lookup.
                return null;
            }
        }
        return name;
    }

    private static boolean hasMagic(ByteBuffer datagram) {
        for (int i = 0; i < WireFormat.MAGIC.length; i++) {
            if (datagram.get(WireFormat.OFFSET_MAGIC + i) != WireFormat.MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * The name a foreign datagram claims, for the log line. Naming it is the entire reason the
     * field is stored readable rather than hashed: "expected orders, got ordres" diagnoses a typo
     * instantly, where a hash mismatch would not.
     */
    private static String readApplicationName(ByteBuffer datagram) {
        byte[] raw = new byte[WireFormat.APPLICATION_BYTES];
        int length = 0;
        for (int i = 0; i < raw.length; i++) {
            raw[i] = datagram.get(WireFormat.OFFSET_APPLICATION + i);
            if (raw[i] != 0) {
                length = i + 1;
            }
        }
        return new String(raw, 0, length, StandardCharsets.UTF_8);
    }

    /**
     * Runs the pipeline over one datagram, positioned at zero and limited to its length.
     *
     * @return whether the message was dispatched. A false return has already been counted, and
     * logged at debug; the counter is the signal, because logging every dropped datagram at
     * a visible level would turn a flood into a second denial of service.
     */
    public boolean decode(ByteBuffer datagram, MessageHandler handler) {
        int length = datagram.limit();

        if (length < WireFormat.HEADER_BYTES || length > WireFormat.MAX_DATAGRAM_BYTES) {
            return drop(DropReason.MALFORMED, "length {}", length);
        }
        if (!authenticator.verify(datagram)) {
            return drop(DropReason.BAD_TAG, "authentication failed", null);
        }
        if (!hasMagic(datagram)) {
            return drop(DropReason.BAD_MAGIC, "not a primos datagram", null);
        }
        byte version = datagram.get(WireFormat.OFFSET_VERSION);
        if (version != WireFormat.VERSION) {
            return drop(DropReason.UNKNOWN_VERSION, "version {}", version);
        }
        if (!hasExpectedApplication(datagram)) {
            return drop(DropReason.WRONG_APPLICATION, "addressed to application '{}'",
                    readApplicationName(datagram));
        }
        MessageType type = MessageType.forCode(datagram.get(WireFormat.OFFSET_TYPE) & 0xFF);
        if (type == null) {
            return drop(DropReason.UNKNOWN_TYPE, "type code {}",
                    datagram.get(WireFormat.OFFSET_TYPE) & 0xFF);
        }
        int expectedLength = type.messageBytes() + authenticator.tagBytes();
        if (length != expectedLength) {
            return drop(DropReason.UNKNOWN_TYPE, "{} must be " + expectedLength + " bytes, got {}",
                    type, length);
        }
        int senderId = datagram.getInt(WireFormat.OFFSET_SENDER);
        if (!knownSender.test(senderId)) {
            return drop(DropReason.UNKNOWN_PEER, "sender {}", senderId);
        }

        lastSenderId = senderId;
        dispatch(datagram, type, senderId, handler);
        return true;
    }

    private void dispatch(ByteBuffer datagram, MessageType type, int senderId, MessageHandler handler) {
        long term = datagram.getLong(WireFormat.OFFSET_TERM);
        int flags = datagram.getShort(WireFormat.OFFSET_FLAGS) & 0xFFFF;
        boolean preVote = (flags & WireFormat.FLAG_PRE_VOTE) != 0;

        switch (type) {
            case REQUEST_VOTE:
                handler.onRequestVote(senderId, term, preVote);
                break;
            case REQUEST_VOTE_REPLY:
                handler.onRequestVoteReply(senderId, term,
                        datagram.get(WireFormat.HEADER_BYTES) != 0, preVote);
                break;
            case APPEND_ENTRIES:
                handler.onAppendEntries(senderId, term);
                break;
            case APPEND_ENTRIES_REPLY:
                handler.onAppendEntriesReply(senderId, term,
                        datagram.get(WireFormat.HEADER_BYTES) != 0);
                break;
            case PING:
                handler.onPing(senderId, term, datagram.getLong(WireFormat.HEADER_BYTES));
                break;
            case PING_REPLY:
                handler.onPingReply(senderId, term, datagram.getLong(WireFormat.HEADER_BYTES));
                break;
            case TIMEOUT_NOW:
                handler.onTimeoutNow(senderId, term);
                break;
            case LEAVING:
                handler.onLeaving(senderId, term);
                break;
            default:
                throw new IllegalStateException("unhandled message type " + type);
        }
    }

    private boolean hasExpectedApplication(ByteBuffer datagram) {
        for (int i = 0; i < WireFormat.APPLICATION_BYTES; i++) {
            if (datagram.get(WireFormat.OFFSET_APPLICATION + i) != expectedApplication[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean drop(DropReason reason, String detail, Object argument) {
        drops.record(reason);
        log.warn("dropped datagram ({}): {},{}", reason, detail, argument);
        return false;
    }

    private boolean drop(DropReason reason, String detail, Object first, Object second) {
        drops.record(reason);
        log.warn("dropped datagram ({}): {},{},{}", reason, detail, first, second);
        return false;
    }
}
