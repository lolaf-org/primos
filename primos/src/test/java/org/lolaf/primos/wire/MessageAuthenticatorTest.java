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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class MessageAuthenticatorTest {

    private static final byte[] SECRET = "a-shared-cluster-secret-value".getBytes();

    private final ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    @Test
    void disabled_adds_nothing_and_accepts_anything() {
        MessageAuthenticator authenticator = MessageAuthenticator.disabled();

        assertThat(authenticator.isEnabled()).isFalse();
        assertThat(authenticator.tagBytes()).isZero();
        assertThat(authenticator.verify(ByteBuffer.allocate(52))).isTrue();
    }

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
}
