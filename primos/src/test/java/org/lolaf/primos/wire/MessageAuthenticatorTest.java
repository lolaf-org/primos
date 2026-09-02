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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageAuthenticatorTest {

    private static final byte[] SECRET = "a-shared-cluster-secret-value".getBytes();

    private final ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static byte[] secret(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * A freshly tagged datagram, positioned and limited the way a received one would be.
     */
    private static java.nio.ByteBuffer tagged(MessageAuthenticator authenticator) {
        return new MessageEncoder("orders", 1, authenticator).requestVote(
                java.nio.ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES), 3L, false);
    }

    @Test
    void disabled_adds_nothing_and_accepts_anything() {
        MessageAuthenticator authenticator = MessageAuthenticator.disabled();

        assertThat(authenticator.isEnabled()).isFalse();
        assertThat(authenticator.tagBytes()).isZero();
        assertThat(authenticator.verify(ByteBuffer.allocate(52))).isTrue();
    }

    // ---------------------------------------------------------------- the algorithm

    @Test
    void the_secret_never_appears_in_the_datagram_it_authenticates() {
        MessageEncoder encoder = new MessageEncoder("orders", 3,
                MessageAuthenticator.hmacSha256(SECRET));

        ByteBuffer datagram = encoder.ping(buffer, 7, 1);
        byte[] bytes = new byte[datagram.remaining()];
        datagram.duplicate().get(bytes);

        assertThat(indexOf(bytes, SECRET))
                .as("a tag is a keyed digest, never the key")
                .isEqualTo(-1);
    }

    @Test
    void a_different_secret_cannot_produce_an_acceptable_tag() {
        MessageEncoder forger = new MessageEncoder("orders", 3,
                MessageAuthenticator.hmacSha256("wrong-secret".getBytes()));
        MessageAuthenticator ours = MessageAuthenticator.hmacSha256(SECRET);

        assertThat(ours.verify(forger.ping(buffer, 7, 1))).isFalse();
    }

    @Test
    void the_same_secret_verifies_its_own_tag() {
        MessageEncoder encoder = new MessageEncoder("orders", 3,
                MessageAuthenticator.hmacSha256(SECRET));

        assertThat(MessageAuthenticator.hmacSha256(SECRET).verify(encoder.ping(buffer, 7, 1)))
                .isTrue();
    }

    @Test
    void another_algorithm_tags_with_its_own_length() {
        MessageAuthenticator sha512 = MessageAuthenticator.hmac("HmacSHA512", SECRET);
        MessageEncoder encoder = new MessageEncoder("orders", 3,
                MessageAuthenticator.hmac("HmacSHA512", SECRET));

        assertThat(sha512.tagBytes()).isEqualTo(64);
        assertThat(sha512.algorithm()).isEqualTo("HmacSHA512");
        assertThat(sha512.verify(encoder.ping(buffer, 7, 1))).isTrue();
    }

    @Test
    void the_algorithm_must_match_across_the_cluster() {
        // Nothing on the wire says which algorithm produced a tag, so a node configured with the
        // wrong one is indistinguishable from a node with the wrong secret: everything is dropped.
        MessageEncoder theirs = new MessageEncoder("orders", 3,
                MessageAuthenticator.hmac("HmacSHA512", SECRET));

        assertThat(MessageAuthenticator.hmacSha256(SECRET).verify(theirs.ping(buffer, 7, 1)))
                .isFalse();
    }

    @Test
    void the_default_is_hmac_sha256() {
        MessageAuthenticator authenticator = MessageAuthenticator.hmacSha256(SECRET);

        assertThat(authenticator.algorithm()).isEqualTo(MessageAuthenticator.DEFAULT_ALGORITHM);
        assertThat(authenticator.tagBytes()).isEqualTo(WireFormat.DEFAULT_TAG_BYTES);
    }

    @Test
    void an_algorithm_this_platform_does_not_have_is_refused_by_name() {
        assertThatThrownBy(() -> MessageAuthenticator.hmac("HmacSHA3-512-ish", SECRET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HmacSHA3-512-ish");
    }

    @Test
    void a_tag_too_short_to_defend_anything_is_refused() {
        // No standard Mac is this short, so the guard needs a provider that is: without one the
        // check would be a rule nothing can break, which is no rule at all.
        java.security.Security.addProvider(new StubProvider());
        try {
            assertThatThrownBy(() -> MessageAuthenticator.hmac(StubProvider.ALGORITHM, SECRET))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("8-byte tag");
        } finally {
            java.security.Security.removeProvider(StubProvider.NAME);
        }
    }

    @Test
    void verification_survives_being_called_repeatedly_on_the_same_instance() {
        MessageAuthenticator authenticator = MessageAuthenticator.hmacSha256(SECRET);
        MessageEncoder encoder = new MessageEncoder("orders", 3,
                MessageAuthenticator.hmacSha256(SECRET));

        for (int i = 0; i < 100; i++) {
            assertThat(authenticator.verify(encoder.ping(buffer, i, i))).isTrue();
        }
    }

    @Test
    void a_datagram_shorter_than_a_tag_is_rejected_rather_than_read_off_the_end() {
        assertThat(MessageAuthenticator.hmacSha256(SECRET).verify(ByteBuffer.allocate(8)))
                .isFalse();
    }

    @Test
    void an_empty_secret_is_refused() {
        assertThatThrownBy(() -> MessageAuthenticator.hmacSha256(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    // --------------------------------------------------------- secret rotation

    @Test
    void a_rotating_authenticator_accepts_both_the_current_and_the_previous_secret() {
        MessageAuthenticator onlyOld = MessageAuthenticator.hmacSha256(secret("old"));
        MessageAuthenticator onlyNew = MessageAuthenticator.hmacSha256(secret("new"));
        MessageAuthenticator rotating =
                MessageAuthenticator.hmacSha256(secret("new"), secret("old"));

        assertThat(rotating.verify(tagged(onlyOld)))
                .as("without this, changing a secret means restarting every node at the same"
                        + " instant, which is why secrets go unrotated for years")
                .isTrue();
        assertThat(rotating.verify(tagged(onlyNew))).isTrue();
        assertThat(rotating.hasPreviousSecret()).isTrue();
    }

    @Test
    void a_rotating_authenticator_tags_with_the_current_secret_only() {
        MessageAuthenticator rotating =
                MessageAuthenticator.hmacSha256(secret("new"), secret("old"));

        assertThat(MessageAuthenticator.hmacSha256(secret("new")).verify(tagged(rotating)))
                .as("the window is for accepting the old secret, not for continuing to use it")
                .isTrue();
        assertThat(MessageAuthenticator.hmacSha256(secret("old")).verify(tagged(rotating)))
                .isFalse();
    }

    @Test
    void a_third_secret_is_still_refused_during_a_rotation() {
        MessageAuthenticator rotating =
                MessageAuthenticator.hmacSha256(secret("new"), secret("old"));

        assertThat(rotating.verify(tagged(MessageAuthenticator.hmacSha256(secret("neither")))))
                .as("a rotation window widens the key set by exactly one, and not to anything")
                .isFalse();
    }

    @Test
    void once_the_previous_secret_is_removed_it_stops_being_accepted() {
        MessageAuthenticator afterRotation = MessageAuthenticator.hmacSha256(secret("new"));

        assertThat(afterRotation.verify(tagged(MessageAuthenticator.hmacSha256(secret("old")))))
                .as("a compromised old secret works until the second edit, and that is the whole"
                        + " reason the window is meant to be closed rather than left open")
                .isFalse();
        assertThat(afterRotation.hasPreviousSecret()).isFalse();
    }

    @Test
    void an_empty_previous_secret_is_refused_like_an_empty_current_one() {
        assertThatThrownBy(() -> MessageAuthenticator.hmacSha256(secret("new"), new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    /**
     * A Mac with a tag far too short to be worth having, so the floor has something to refuse.
     */
    private static final class StubProvider extends java.security.Provider {

        private static final String NAME = "primos-test-stub";
        private static final String ALGORITHM = "HmacTruncated64";

        private StubProvider() {
            super(NAME, "1.0", "an eight-byte Mac, for testing the tag-length floor");
            put("Mac." + ALGORITHM, ShortMac.class.getName());
        }
    }

    /**
     * Public and with a public constructor because the JCE instantiates it reflectively.
     */
    public static final class ShortMac extends javax.crypto.MacSpi {

        public ShortMac() {
        }

        @Override
        protected int engineGetMacLength() {
            return 8;
        }

        @Override
        protected void engineInit(java.security.Key key,
                                  java.security.spec.AlgorithmParameterSpec params) {
        }

        @Override
        protected void engineUpdate(byte input) {
        }

        @Override
        protected void engineUpdate(byte[] input, int offset, int length) {
        }

        @Override
        protected byte[] engineDoFinal() {
            return new byte[8];
        }

        @Override
        protected void engineReset() {
        }
    }

    // ------------------------------------------------------------------ keyring

    @Test
    void a_keyring_accepts_a_datagram_tagged_with_any_of_its_keys() {
        MessageAuthenticator ring = MessageAuthenticator.keyring(Arrays.asList(
                MessageAuthenticator.KeySpec.of("platform", null, bytes("k1")),
                MessageAuthenticator.KeySpec.of("orders", null, bytes("k2"))));

        assertThat(ring.verify(taggedWith(bytes("k1"), null))).isTrue();
        assertThat(ring.verify(taggedWith(bytes("k2"), null))).isTrue();
        assertThat(ring.verify(taggedWith(bytes("not-in-the-ring"), null)))
                .as("a ring is not a way in for a key nobody put in it")
                .isFalse();
    }

    @Test
    void the_key_that_verified_is_the_one_that_signs_and_the_one_that_is_named() {
        MessageAuthenticator ring = MessageAuthenticator.keyring(Arrays.asList(
                MessageAuthenticator.KeySpec.of("platform", null, bytes("k1")),
                MessageAuthenticator.KeySpec.of("orders", "HmacSHA512", bytes("k2"))));

        assertThat(ring.verify(taggedWith(bytes("k2"), "HmacSHA512"))).isTrue();
        assertThat(ring.keyName())
                .as("which key admitted a tenant is the whole of the diagnosis")
                .isEqualTo("orders");
        assertThat(ring.algorithm()).isEqualTo("HmacSHA512");
        assertThat(ring.tagBytes())
                .as("the expected datagram length follows the key that matched, not the first one")
                .isEqualTo(64);
    }

    /**
     * The selection is a cache, not a commitment. Pinning an application to the key it was first
     * seen with would reject the new one on the day it changed — the day a ring exists for.
     */
    @Test
    void a_key_that_stops_matching_falls_back_to_the_rest_of_the_ring() {
        MessageAuthenticator ring = MessageAuthenticator.keyring(Arrays.asList(
                MessageAuthenticator.KeySpec.of("old", null, bytes("k1")),
                MessageAuthenticator.KeySpec.of("new", null, bytes("k2"))));

        assertThat(ring.verify(taggedWith(bytes("k1"), null))).isTrue();
        assertThat(ring.keyName()).isEqualTo("old");

        assertThat(ring.verify(taggedWith(bytes("k2"), null)))
                .as("rotation is the ring's whole point; a pinned key would break on the day it"
                        + " changed")
                .isTrue();
        assertThat(ring.keyName()).isEqualTo("new");
        assertThat(ring.verify(taggedWith(bytes("k1"), null)))
                .as("and the old one still works, which is what makes the window a window")
                .isTrue();
    }

    @Test
    void a_mixed_ring_does_not_confuse_one_algorithms_tag_for_anothers() {
        MessageAuthenticator ring = MessageAuthenticator.keyring(Arrays.asList(
                MessageAuthenticator.KeySpec.of("short", "HmacSHA256", bytes("k1")),
                MessageAuthenticator.KeySpec.of("long", "HmacSHA512", bytes("k2"))));

        assertThat(ring.verify(taggedWith(bytes("k1"), "HmacSHA256"))).isTrue();
        assertThat(ring.tagBytes()).isEqualTo(32);
        assertThat(ring.verify(taggedWith(bytes("k2"), "HmacSHA512"))).isTrue();
        assertThat(ring.tagBytes()).isEqualTo(64);
    }

    @Test
    void an_empty_keyring_is_refused_rather_than_silently_disabling_authentication() {
        assertThatThrownBy(() -> MessageAuthenticator.keyring(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one key");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** A real RequestVote tagged with one key, which is what a ring has to sort out. */
    private static ByteBuffer taggedWith(byte[] secret, String algorithm) {
        MessageAuthenticator one = algorithm == null
                ? MessageAuthenticator.hmacSha256(secret)
                : MessageAuthenticator.hmac(algorithm, secret);
        return new MessageEncoder("orders", 1, one)
                .requestVote(ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES), 1L, false);
    }
}
