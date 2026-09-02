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
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.RecordingStats;

/**
 * Throws hostile bytes at the parser.
 *
 * <p>This parser is reachable by anyone who can send a UDP packet to the port, so the property
 * being asserted is not that it decodes anything in particular but that it never does anything
 * else: no exception escapes, and nothing is dispatched that was not a genuine message.
 *
 * <p>Seeded, so a failure is reproducible from the output rather than being a story about a build
 * that once went red.
 */
class ParserFuzzTest {

    private static final String APP = "test-app";
    private static final int SENDER = 3;
    private static final int ITERATIONS = 20_000;

    private final RecordingStats drops = new RecordingStats();
    private final RecordingHandler handler = new RecordingHandler();
    private final MessageDecoder decoder = new MessageDecoder(APP,
            MessageAuthenticator.disabled(), id -> id == SENDER, drops);

    @Test
    void random_bytes_are_never_dispatched_and_never_throw() {
        Random random = new Random(20260828L);

        assertThatCode(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] noise = new byte[random.nextInt(WireFormat.MAX_DATAGRAM_BYTES + 64)];
                random.nextBytes(noise);
                decoder.decode(ByteBuffer.wrap(noise), handler);
            }
        }).doesNotThrowAnyException();

        assertThat(handler.received)
                .as("random bytes cannot form a valid message from a configured peer")
                .isEmpty();
        assertThat(drops.totalDrops()).isEqualTo(ITERATIONS);
    }

    @Test
    void mutating_one_byte_of_a_real_message_is_either_valid_or_counted_never_an_exception() {
        Random random = new Random(4242L);
        MessageEncoder encoder = new MessageEncoder(APP, SENDER, MessageAuthenticator.disabled());
        ByteBuffer scratch = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

        assertThatCode(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                ByteBuffer valid = encoder.ping(scratch, random.nextLong(), random.nextLong());
                byte[] copy = new byte[valid.remaining()];
                valid.duplicate().get(copy);

                int index = random.nextInt(copy.length);
                copy[index] = (byte) random.nextInt(256);

                long dispatched = handler.received.size();
                boolean accepted = decoder.decode(ByteBuffer.wrap(copy), handler);

                assertThat(handler.received.size()).isEqualTo(dispatched + (accepted ? 1 : 0));
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void truncating_a_real_message_at_every_length_is_always_rejected_cleanly() {
        MessageEncoder encoder = new MessageEncoder(APP, SENDER, MessageAuthenticator.disabled());
        ByteBuffer scratch = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
        ByteBuffer valid = encoder.ping(scratch, 7, 11);
        byte[] full = new byte[valid.remaining()];
        valid.duplicate().get(full);

        for (int length = 0; length < full.length; length++) {
            byte[] truncated = new byte[length];
            System.arraycopy(full, 0, truncated, 0, length);

            assertThat(decoder.decode(ByteBuffer.wrap(truncated), handler))
                    .as("a %d-byte prefix of a %d-byte ping", length, full.length)
                    .isFalse();
        }

        assertThat(handler.received).isEmpty();
    }
}
