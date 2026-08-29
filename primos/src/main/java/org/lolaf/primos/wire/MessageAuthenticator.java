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
import java.security.MessageDigest;
import java.util.Objects;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Appends and verifies the HMAC-SHA256 tag that proves a datagram came from a cluster member.
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

    private static final String ALGORITHM = "HmacSha256";

    private final Mac mac;
    private final Mac previousMac;
    private final byte[] computed;
    private final byte[] received;

    private MessageAuthenticator(Mac mac, Mac previousMac) {
        this.mac = mac;
        this.previousMac = previousMac;
        this.computed = mac == null ? null : new byte[WireFormat.TAG_BYTES];
        this.received = mac == null ? null : new byte[WireFormat.TAG_BYTES];
    }

    /**
     * No authentication. Appropriate only on a trusted private network, and never the documented
     * default: without it, a forged reply is a route to a false leader rather than merely a
     * nuisance.
     */
    public static MessageAuthenticator disabled() {
        return new MessageAuthenticator(null, null);
    }

    /**
     * Authentication with a shared cluster secret.
     *
     * <p>The key is initialised once, because the key schedule is the expensive part of HMAC;
     * tagging a datagram thereafter costs roughly a microsecond, against heartbeats every tens of
     * milliseconds.
     */
    public static MessageAuthenticator hmacSha256(byte[] secret) {
        return hmacSha256(secret, null);
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
        Objects.requireNonNull(secret, "secret");
        if (secret.length == 0) {
            throw new IllegalArgumentException("the cluster secret must not be empty");
        }
        if (previousSecret != null && previousSecret.length == 0) {
            throw new IllegalArgumentException("the previous cluster secret must not be empty");
        }
        return new MessageAuthenticator(keyed(secret), previousSecret == null ? null : keyed(previousSecret));
    }

    private static Mac keyed(byte[] secret) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            return mac;
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("HmacSha256 is required by every Java platform", e);
        }
    }

    /** Whether a secret is being rotated out and still accepted. */
    public boolean hasPreviousSecret() {
        return previousMac != null;
    }

    /** Whether datagrams carry a tag. */
    public boolean isEnabled() {
        return mac != null;
    }

    /** Bytes this adds to every datagram: 32 when enabled, 0 when not. */
    public int tagBytes() {
        return mac == null ? 0 : WireFormat.TAG_BYTES;
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
        int bodyEnd = limit - WireFormat.TAG_BYTES;
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
}
