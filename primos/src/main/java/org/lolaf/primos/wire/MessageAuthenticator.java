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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Appends and verifies the tag that proves a datagram came from a cluster member.
 *
 * <p>HMAC-SHA256 unless a cluster chooses otherwise — see {@link #hmac(String, byte[], byte[])},
 * and note that the choice is local policy every member has to share, because nothing on the wire
 * says which algorithm was used.
 *
 * <p>The threat this closes is off-path injection. UDP source addresses are freely spoofable, so
 * "is this from a configured peer?" is not a security control on its own: an attacker who guesses
 * the port can inject a message carrying an enormous term, and every node that sees it steps down
 * and adopts that term. That is a permanent, trivially repeatable denial of service. The reply
 * direction matters at least as much — a forged vote grant plus a candidate's own vote is a
 * majority in a three-voter cluster, which manufactures a leader outright.
 *
 * <p><strong>There is deliberately no "authentication present" flag in the header.</strong> If a
 * datagram declared whether it was authenticated, an attacker would clear the bit and strip
 * authentication — a downgrade attack. Whether a tag is expected is local policy, and the length
 * check enforces it: an unauthenticated datagram is simply the wrong size for a node expecting 32
 * more bytes.
 *
 * <p>What this does not provide: confidentiality (it authenticates, it does not encrypt) and
 * replay protection (a captured datagram resent verbatim still verifies). Replay is absorbed by
 * the protocol rather than here — old-term replays fail the term check, current-term replays are
 * duplicates UDP already forced us to tolerate, and a replayed vote grant is a no-op because votes
 * are counted in a set.
 *
 * <p>Not thread-safe: {@link Mac} is stateful, and each instance is owned by one node thread.
 */
public final class MessageAuthenticator {

    /**
     * What a cluster authenticates with unless it says otherwise. Every Java platform has it, its
     * tag is the 32 bytes the wire format was drawn around, and nothing has been found wrong with
     * it — a cluster that has no opinion should keep this one.
     */
    public static final String DEFAULT_ALGORITHM = "HmacSHA256";

    /**
     * The shortest tag this will accept, whatever the algorithm produces.
     *
     * <p>128 bits. A tag is the whole of what stops a forged datagram, and a forged reply here does
     * not merely corrupt a message — it manufactures a leader. No standard {@code Mac} is below
     * this; the check exists for the exotic provider that is.
     */
    private static final int MIN_TAG_BYTES = 16;

    private final String algorithm;
    private final Mac mac;
    private final Mac previousMac;
    private final int tagBytes;
    private final byte[] computed;
    private final byte[] received;

    private MessageAuthenticator(String algorithm, Mac mac, Mac previousMac) {
        this.algorithm = algorithm;
        this.mac = mac;
        this.previousMac = previousMac;
        this.tagBytes = mac == null ? 0 : mac.getMacLength();
        this.computed = mac == null ? null : new byte[tagBytes];
        this.received = mac == null ? null : new byte[tagBytes];
    }

    /**
     * No authentication. Appropriate only on a trusted private network, and never the documented
     * default: without it, a forged reply is a route to a false leader rather than merely a
     * nuisance.
     */
    public static MessageAuthenticator disabled() {
        return new MessageAuthenticator(null, null, null);
    }

    /**
     * Authentication with a shared cluster secret.
     *
     * <p>The key is initialised once, because the key schedule is the expensive part of HMAC;
     * tagging a datagram thereafter costs roughly a microsecond, against heartbeats every tens of
     * milliseconds.
     */
    public static MessageAuthenticator hmacSha256(byte[] secret) {
        return hmac(DEFAULT_ALGORITHM, secret, null);
    }

    /**
     * Authentication during a secret rotation: tags with {@code secret}, accepts either.
     *
     * <p>Without this, changing a secret is a flag day — every node in the cluster has to be
     * restarted at the same instant, because the moment one holds the new secret and another holds
     * the old one they stop being able to talk at all. That is the worst possible time to lose a
     * cluster's consensus traffic, and the pressure it creates is what makes people leave secrets
     * unrotated for years.
     *
     * <p>With it, rotation is two edits and a window: set the new secret as current and the old one
     * as previous, wait until every node has been updated, then drop the previous one. Nothing is
     * ever unable to talk, and nothing restarts in lockstep.
     *
     * <p>The cost is that a compromised old secret keeps working until the second edit, which is
     * why the previous secret is a rotation window rather than a permanent second key.
     *
     * @param previousSecret the secret being rotated out, or null when none is
     */
    public static MessageAuthenticator hmacSha256(byte[] secret, byte[] previousSecret) {
        return hmac(DEFAULT_ALGORITHM, secret, previousSecret);
    }

    /**
     * Authentication with a chosen {@link Mac} algorithm — any name the platform's JCE knows, such
     * as {@code HmacSHA256} or {@code HmacSHA512}.
     *
     * <p><strong>The algorithm is not on the wire, and deliberately so.</strong> A datagram that
     * announced how it was authenticated would let an attacker rewrite that field and pick the
     * weakest thing both ends accept, which is the downgrade attack the absent
     * "authentication present" flag already avoids. It is local policy instead, exactly like the
     * secret: <em>every node of a cluster, its witness included, must be configured with the same
     * one</em>. A cluster that disagrees with itself here behaves precisely as though it disagreed
     * about the secret — every datagram fails verification and is counted under
     * {@link org.lolaf.primos.DropReason#BAD_TAG}, which is the number to look at when a cluster
     * that should be talking is silent.
     *
     * <p>The tag length follows the algorithm rather than the wire format: 32 bytes for the
     * default, 64 for {@code HmacSHA512}, 20 for {@code HmacSHA1}. Everything that measures a
     * datagram asks {@link #tagBytes()}, so a longer tag costs bytes on the wire and nothing else.
     *
     * @param algorithm      a JCE {@code Mac} algorithm name; {@link #DEFAULT_ALGORITHM} unless
     *                       there is a reason to differ
     * @param secret         the cluster secret, which this tags with
     * @param previousSecret the secret being rotated out and still accepted, or null when none is
     * @throws IllegalArgumentException if the platform has no such algorithm, or its tag is too
     *                                  short or too long to carry
     */
    public static MessageAuthenticator hmac(String algorithm, byte[] secret, byte[] previousSecret) {
        Objects.requireNonNull(algorithm, "algorithm");
        Objects.requireNonNull(secret, "secret");
        if (secret.length == 0) {
            throw new IllegalArgumentException("the cluster secret must not be empty");
        }
        if (previousSecret != null && previousSecret.length == 0) {
            throw new IllegalArgumentException("the previous cluster secret must not be empty");
        }
        Mac mac = keyed(algorithm, secret);
        requireCarriableTag(algorithm, mac.getMacLength());
        return new MessageAuthenticator(algorithm, mac,
                previousSecret == null ? null : keyed(algorithm, previousSecret));
    }

    /**
     * Authentication with a chosen algorithm and no rotation window. See
     * {@link #hmac(String, byte[], byte[])}.
     */
    public static MessageAuthenticator hmac(String algorithm, byte[] secret) {
        return hmac(algorithm, secret, null);
    }

    private static Mac keyed(String algorithm, byte[] secret) {
        Mac mac;
        try {
            mac = Mac.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            // A configuration error, not a platform failure: the name came from whoever built the
            // node, so it is named back to them.
            throw new IllegalArgumentException("this Java platform has no Mac algorithm named '"
                    + algorithm + "'; " + DEFAULT_ALGORITHM + " is always available", e);
        }
        try {
            mac.init(new SecretKeySpec(secret, algorithm));
        } catch (InvalidKeyException e) {
            throw new IllegalArgumentException(
                    "the cluster secret is not a usable key for " + algorithm, e);
        }
        return mac;
    }

    /**
     * Refuses a tag that is too short to be worth having or too long to fit beside the largest
     * message, so both are a configuration error at startup rather than a cluster that cannot talk.
     */
    private static void requireCarriableTag(String algorithm, int tagBytes) {
        if (tagBytes < MIN_TAG_BYTES) {
            throw new IllegalArgumentException(algorithm + " produces a " + tagBytes
                    + "-byte tag, and primos requires at least " + MIN_TAG_BYTES
                    + ": a forged datagram here manufactures a leader, so the tag is the whole of"
                    + " the defence");
        }
        int largestMessage = 0;
        for (MessageType type : MessageType.values()) {
            largestMessage = Math.max(largestMessage, type.messageBytes());
        }
        int budget = WireFormat.MAX_DATAGRAM_BYTES - largestMessage;
        if (tagBytes > budget) {
            throw new IllegalArgumentException(algorithm + " produces a " + tagBytes
                    + "-byte tag, and only " + budget + " bytes are left beside the largest message"
                    + " within the " + WireFormat.MAX_DATAGRAM_BYTES
                    + "-byte datagram primos keeps under to avoid IP fragmentation");
        }
    }

    /**
     * The algorithm datagrams are tagged with, or null when authentication is disabled. Worth
     * logging at startup: nothing on the wire carries it, so a cluster that disagrees about it
     * looks exactly like one that disagrees about the secret.
     */
    public String algorithm() {
        return algorithm;
    }

    /**
     * Hashes {@code [0, length)} of the buffer without allocating: the range is selected by moving
     * the buffer's own position and limit rather than by taking a duplicate or a copy.
     */
    private static void computeInto(Mac mac, ByteBuffer datagram, int length, byte[] out) {
        int savedLimit = datagram.limit();
        datagram.limit(length);
        datagram.position(0);
        mac.update(datagram);
        datagram.limit(savedLimit);
        try {
            mac.doFinal(out, 0);
        } catch (javax.crypto.ShortBufferException e) {
            throw new IllegalStateException("a 32-byte buffer always fits an HMAC-SHA256 tag", e);
        }
    }

    /**
     * Whether a secret is being rotated out and still accepted.
     */
    public boolean hasPreviousSecret() {
        return previousMac != null;
    }

    /**
     * Whether datagrams carry a tag.
     */
    public boolean isEnabled() {
        return mac != null;
    }

    /**
     * Bytes this adds to every datagram: the algorithm's tag length when enabled, 0 when not.
     */
    public int tagBytes() {
        return tagBytes;
    }

    /**
     * Tags everything written to the buffer so far, appending the tag at the current position.
     *
     * <p>The tag covers the <em>entire</em> datagram, magic and version included, so no field is
     * malleable.
     */
    public void appendTag(ByteBuffer datagram) {
        if (mac == null) {
            return;
        }
        int end = datagram.position();
        computeInto(mac, datagram, end, computed);
        datagram.position(end);
        datagram.put(computed);
    }

    /**
     * Whether a received datagram carries a valid tag over its own contents.
     *
     * <p>Called before any field is parsed, so attacker-controlled bytes never reach the parser.
     * The comparison is {@link MessageDigest#isEqual}, which is time-constant, rather than
     * {@code Arrays.equals}, which would leak the tag one byte at a time.
     */
    public boolean verify(ByteBuffer datagram) {
        if (mac == null) {
            return true;
        }
        int limit = datagram.limit();
        int bodyEnd = limit - tagBytes;
        if (bodyEnd < 0) {
            return false;
        }
        datagram.position(bodyEnd);
        datagram.get(received);

        computeInto(mac, datagram, bodyEnd, computed);
        if (MessageDigest.isEqual(computed, received)) {
            return true;
        }
        if (previousMac == null) {
            return false;
        }
        // Only reached during a rotation window, so the second HMAC is not on any steady-state
        // path. Both comparisons are time-constant, so failing the first one leaks nothing about
        // how far the second got.
        computeInto(previousMac, datagram, bodyEnd, computed);
        return MessageDigest.isEqual(computed, received);
    }
}
