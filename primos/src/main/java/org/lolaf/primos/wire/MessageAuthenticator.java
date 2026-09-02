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
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
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

    /**
     * The keys this accepts, in the order they were given. Empty when authentication is disabled.
     *
     * <p>One key is the ordinary case, two is a rotation window, and more is a witness server
     * holding a key per tenant. They are the same mechanism: a datagram is authentic if any key
     * here accepts it, and everything this class writes is written with whichever one last did.
     */
    private final Key[] keys;

    /**
     * Which key last accepted a datagram, and therefore which one signs the next.
     *
     * <p><strong>A cache, never a commitment.</strong> Verification always falls back to the rest
     * of the ring, and that is what makes rotation work: an application pinned to the key it was
     * first seen with would reject the new one on the day it changed, which is precisely the day
     * the ring exists for.
     */
    private int selected;

    private MessageAuthenticator(Key[] keys) {
        this.keys = keys;
        this.selected = 0;
    }

    /**
     * One key: its algorithm, its {@code Mac} and the scratch buffers that keep verification
     * allocation-free.
     */
    private static final class Key {

        private final String name;
        private final String algorithm;
        private final Mac mac;
        private final int tagBytes;
        private final byte[] computed;
        private final byte[] received;

        private Key(String name, String algorithm, Mac mac) {
            this.name = name;
            this.algorithm = algorithm;
            this.mac = mac;
            this.tagBytes = mac.getMacLength();
            this.computed = new byte[tagBytes];
            this.received = new byte[tagBytes];
        }
    }

    /**
     * The algorithm in force, or null when authentication is disabled.
     *
     * <p>Worth logging at startup: nothing on the wire carries it, so a party that disagrees about
     * it looks exactly like one that disagrees about the secret. With several keys this names the
     * one that last verified, which is the one being used.
     */
    public String algorithm() {
        return keys.length == 0 ? null : keys[selected].algorithm;
    }

    /**
     * The name of the key that last verified something, or null when it has none.
     *
     * <p>Only a witness server names its keys, and it does so for one reason: a log line saying
     * <em>which</em> key admitted a tenant is the difference between a diagnosable configuration
     * and a working one nobody can explain.
     */
    public String keyName() {
        return keys.length == 0 ? null : keys[selected].name;
    }

    /**
     * Bytes this adds to every datagram: the tag length of the key in force, 0 when disabled.
     *
     * <p><strong>Read this only after {@link #verify}</strong>, which is what selects the key. The
     * receive pipeline does exactly that — it verifies, then checks the length against the type —
     * and with a ring of mixed algorithms that ordering stops being tidiness and becomes load
     * bearing, because the expected length depends on which key accepted the datagram.
     */
    public int tagBytes() {
        return keys.length == 0 ? 0 : keys[selected].tagBytes;
    }

    /**
     * No authentication. Appropriate only on a trusted private network, and never the documented
     * default: without it, a forged reply is a route to a false leader rather than merely a
     * nuisance.
     */
    public static MessageAuthenticator disabled() {
        return new MessageAuthenticator(new Key[0]);
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
        Key current = key(null, algorithm, secret);
        if (previousSecret == null) {
            return new MessageAuthenticator(new Key[] {current});
        }
        return new MessageAuthenticator(new Key[] {current, key(null, algorithm, previousSecret)});
    }

    /**
     * Authentication against several keys at once, accepting any of them.
     *
     * <p>This is what {@link #hmac(String, byte[], byte[])} already was — a current key and its
     * predecessor are a two-key ring — generalised to the case a witness server has: <em>keys
     * belonging to different parties</em>, not one key and its own past. A datagram is authentic
     * if any key accepts it, and replies are signed with whichever key last did.
     *
     * <p>Two consequences worth knowing before relying on it. <strong>A ring is not isolation:</strong>
     * any key in it verifies any datagram given to this authenticator, so whoever decides which
     * keys go in a ring is deciding who can speak for whom — a caller wanting one party's traffic
     * checked against only that party's key must build a ring holding only that key. And
     * <strong>the keys may use different algorithms</strong>, so the tag length varies with the key
     * that matched; see {@link #tagBytes()}.
     *
     * @param keys at least one, each a name for logs (may be null), a JCE {@code Mac} algorithm
     *             and a secret
     * @throws IllegalArgumentException if the list is empty, or any entry names an algorithm the
     *                                  platform lacks or whose tag will not carry
     */
    public static MessageAuthenticator keyring(List<KeySpec> keys) {
        Objects.requireNonNull(keys, "keys");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("a keyring needs at least one key;"
                    + " use disabled() to run without authentication, which is never advisable");
        }
        Key[] ring = new Key[keys.size()];
        for (int i = 0; i < ring.length; i++) {
            KeySpec spec = keys.get(i);
            ring[i] = key(spec.name, spec.algorithm, spec.secret);
        }
        return new MessageAuthenticator(ring);
    }

    /**
     * One entry for {@link #keyring(List)}: a name for logs, an algorithm and a secret.
     */
    public static final class KeySpec {

        private final String name;
        private final String algorithm;
        private final byte[] secret;

        private KeySpec(String name, String algorithm, byte[] secret) {
            this.name = name;
            this.algorithm = algorithm == null ? DEFAULT_ALGORITHM : algorithm;
            this.secret = secret;
        }

        /**
         * @param name      what to call this key in a log line, or null when it has no name
         * @param algorithm a JCE {@code Mac} name, or null for {@link #DEFAULT_ALGORITHM}
         * @param secret    the key itself; not copied, because the caller built it for this
         */
        public static KeySpec of(String name, String algorithm, byte[] secret) {
            return new KeySpec(name, algorithm, Objects.requireNonNull(secret, "secret"));
        }
    }

    private static Key key(String name, String algorithm, byte[] secret) {
        Objects.requireNonNull(algorithm, "algorithm");
        Objects.requireNonNull(secret, "secret");
        if (secret.length == 0) {
            throw new IllegalArgumentException("a cluster secret must not be empty");
        }
        Mac mac = keyed(algorithm, secret);
        requireCarriableTag(algorithm, mac.getMacLength());
        return new Key(name, algorithm, mac);
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
        } catch (ShortBufferException e) {
            throw new IllegalStateException("a 32-byte buffer always fits an HMAC-SHA256 tag", e);
        }
    }

    /**
     * Whether more than one key is accepted — a rotation window, or a witness's several tenants.
     */
    public boolean hasPreviousSecret() {
        return keys.length > 1;
    }

    /**
     * Whether datagrams carry a tag.
     */
    public boolean isEnabled() {
        return keys.length > 0;
    }

    /**
     * Tags everything written to the buffer so far, appending the tag at the current position.
     *
     * <p>The tag covers the <em>entire</em> datagram, magic and version included, so no field is
     * malleable.
     */
    public void appendTag(ByteBuffer datagram) {
        if (keys.length == 0) {
            return;
        }
        Key key = keys[selected];
        int end = datagram.position();
        computeInto(key.mac, datagram, end, key.computed);
        datagram.position(end);
        datagram.put(key.computed);
    }

    /**
     * Whether a received datagram carries a valid tag over its own contents.
     *
     * <p>Called before any field is parsed, so attacker-controlled bytes never reach the parser.
     * The comparison is {@link MessageDigest#isEqual}, which is time-constant, rather than
     * {@code Arrays.equals}, which would leak the tag one byte at a time.
     */
    public boolean verify(ByteBuffer datagram) {
        if (keys.length == 0) {
            return true;
        }
        // The key that worked last time, first. In a healthy steady state it is the only one
        // tried, so a ring costs one HMAC exactly as a single key does; the rest of the ring is
        // reached during a rotation, or by a witness meeting a tenant it has not heard from yet.
        if (accepts(keys[selected], datagram)) {
            return true;
        }
        for (int i = 0; i < keys.length; i++) {
            if (i != selected && accepts(keys[i], datagram)) {
                selected = i;
                return true;
            }
        }
        return false;
    }

    /**
     * Whether one key accepts this datagram, at that key's own tag length.
     *
     * <p>The length test is not only a bounds check. A ring may mix algorithms, and a tag length
     * that would leave less than a header behind cannot be this key's — so a datagram tagged with
     * a 32-byte key is rejected by a 64-byte key without hashing anything, which is what keeps a
     * mixed ring from costing a hash per key on every datagram.
     */
    private static boolean accepts(Key key, ByteBuffer datagram) {
        int bodyEnd = datagram.limit() - key.tagBytes;
        if (bodyEnd < WireFormat.HEADER_BYTES) {
            return false;
        }
        datagram.position(bodyEnd);
        datagram.get(key.received);
        computeInto(key.mac, datagram, bodyEnd, key.computed);
        // Time-constant, so a failure leaks nothing about how far the comparison got.
        return MessageDigest.isEqual(key.computed, key.received);
    }
}
