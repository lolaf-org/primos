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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.DropReason;
import org.lolaf.primos.RecordingStats;

/** One test per step of the pipeline, each asserting its own counter moved and nothing dispatched. */
class ReceivePipelineTest {

    private static final String APP = "test-app";
    private static final int SENDER = 3;
    private static final byte[] SECRET = "shared-cluster-secret".getBytes();

    private final RecordingStats drops = new RecordingStats();
    private final RecordingHandler handler = new RecordingHandler();
    private final ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    private final MessageEncoder encoder =
            new MessageEncoder(APP, SENDER, MessageAuthenticator.disabled());
    private final MessageDecoder decoder = new MessageDecoder(APP,
            MessageAuthenticator.disabled(), id -> id == SENDER, drops);

    @Test
    void step_1_too_short_to_hold_a_header() {
        assertDropped(ByteBuffer.allocate(WireFormat.HEADER_BYTES - 1), DropReason.MALFORMED);
    }

    @Test
    void step_1_larger_than_an_unfragmented_datagram() {
        assertDropped(ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES + 1), DropReason.MALFORMED);
    }

    @Test
    void step_2_a_flipped_bit_anywhere_fails_authentication() {
        MessageEncoder tagging = new MessageEncoder(APP, SENDER,
                MessageAuthenticator.hmacSha256(SECRET));
        MessageDecoder verifying = new MessageDecoder(APP,
                MessageAuthenticator.hmacSha256(SECRET), id -> true, drops);

        for (int index : new int[] {0, 5, WireFormat.OFFSET_TERM, WireFormat.HEADER_BYTES - 1}) {
            ByteBuffer datagram = tagging.ping(buffer, 7, 1);
            datagram.put(index, (byte) (datagram.get(index) ^ 0x01));

            assertThat(verifying.decode(datagram, handler)).isFalse();
        }

        assertThat(drops.drops(DropReason.BAD_TAG)).isEqualTo(4);
        assertThat(handler.received).isEmpty();
    }

    @Test
    void step_2_an_untagged_datagram_is_rejected_by_a_node_expecting_one() {
        MessageDecoder verifying = new MessageDecoder(APP,
                MessageAuthenticator.hmacSha256(SECRET), id -> true, drops);

        assertThat(verifying.decode(encoder.ping(buffer, 7, 1), handler)).isFalse();
        assertThat(handler.received).isEmpty();
    }

    @Test
    void step_3_traffic_that_is_not_primos_at_all() {
        ByteBuffer datagram = encoder.appendEntries(buffer, 1, 0, 0);
        datagram.put(0, (byte) 'X');

        assertDropped(datagram, DropReason.BAD_MAGIC);
    }

    @Test
    void step_3_a_version_this_build_does_not_implement_is_never_guessed_at() {
        ByteBuffer datagram = encoder.appendEntries(buffer, 1, 0, 0);
        datagram.put(WireFormat.OFFSET_VERSION, (byte) 99);

        assertDropped(datagram, DropReason.UNKNOWN_VERSION);
    }

    @Test
    void step_4_a_one_character_typo_is_a_different_cluster() {
        MessageEncoder neighbour = new MessageEncoder("tset-app", SENDER,
                MessageAuthenticator.disabled());

        assertDropped(neighbour.appendEntries(buffer, 1, 0, 0), DropReason.WRONG_APPLICATION);
    }

    @Test
    void step_4_a_name_that_is_a_prefix_of_ours_is_still_a_different_cluster() {
        MessageEncoder neighbour = new MessageEncoder("test-ap", SENDER,
                MessageAuthenticator.disabled());

        assertDropped(neighbour.appendEntries(buffer, 1, 0, 0), DropReason.WRONG_APPLICATION);
    }

    @Test
    void step_4_a_name_that_extends_ours_is_still_a_different_cluster() {
        MessageEncoder neighbour = new MessageEncoder("test-application", SENDER,
                MessageAuthenticator.disabled());

        assertDropped(neighbour.appendEntries(buffer, 1, 0, 0), DropReason.WRONG_APPLICATION);
    }

    @Test
    void step_5_an_unknown_type_code() {
        ByteBuffer datagram = encoder.appendEntries(buffer, 1, 0, 0);
        datagram.put(WireFormat.OFFSET_TYPE, (byte) 0x7F);

        assertDropped(datagram, DropReason.UNKNOWN_TYPE);
    }

    @Test
    void step_5_the_right_type_at_the_wrong_length() {
        ByteBuffer datagram = encoder.ping(buffer, 1, 5);
        datagram.limit(datagram.limit() - 1);

        assertDropped(datagram, DropReason.UNKNOWN_TYPE);
    }

    @Test
    void step_5_a_padded_datagram_is_rejected_as_firmly_as_a_truncated_one() {
        ByteBuffer datagram = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);
        encoder.appendEntries(datagram, 1, 0, 0);
        datagram.limit(datagram.limit() + 4);

        assertDropped(datagram, DropReason.UNKNOWN_TYPE);
    }

    @Test
    void step_6_a_sender_that_is_not_a_configured_peer() {
        MessageEncoder stranger = new MessageEncoder(APP, 999, MessageAuthenticator.disabled());

        assertDropped(stranger.appendEntries(buffer, 1, 0, 0), DropReason.UNKNOWN_PEER);
    }

    @Test
    void a_witness_may_accept_senders_it_has_never_seen() {
        MessageDecoder permissive = new MessageDecoder(APP, MessageAuthenticator.disabled(),
                id -> true, drops);
        MessageEncoder stranger = new MessageEncoder(APP, 999, MessageAuthenticator.disabled());

        assertThat(permissive.decode(stranger.appendEntries(buffer, 1, 0, 0), handler)).isTrue();
        assertThat(handler.only()).isEqualTo("appendEntries(999,1,version=0)");
    }

    @Test
    void authentication_is_checked_before_anything_else_is_believed() {
        MessageDecoder verifying = new MessageDecoder(APP,
                MessageAuthenticator.hmacSha256(SECRET), id -> id == SENDER, drops);
        MessageEncoder forger = new MessageEncoder("ordres", 999,
                MessageAuthenticator.hmacSha256("a-different-secret".getBytes()));

        assertThat(verifying.decode(forger.appendEntries(buffer, 1, 0, 0), handler)).isFalse();

        assertThat(drops.drops(DropReason.BAD_TAG)).isEqualTo(1);
        assertThat(drops.drops(DropReason.WRONG_APPLICATION)).isZero();
        assertThat(drops.drops(DropReason.UNKNOWN_PEER)).isZero();
    }

    @Test
    void the_counters_are_per_reason_and_the_total_adds_up() {
        assertDropped(ByteBuffer.allocate(4), DropReason.MALFORMED);
        ByteBuffer badMagic = encoder.appendEntries(buffer, 1, 0, 0);
        badMagic.put(0, (byte) 'X');
        assertDropped(badMagic, DropReason.BAD_MAGIC);

        assertThat(drops.drops(DropReason.MALFORMED)).isEqualTo(1);
        assertThat(drops.drops(DropReason.BAD_MAGIC)).isEqualTo(1);
        assertThat(drops.totalDrops()).isEqualTo(2);
        assertThat(drops.drops(DropReason.UNKNOWN_PEER))
                .as("a reason nothing tripped is zero, not absent")
                .isZero();
    }

    private void assertDropped(ByteBuffer datagram, DropReason expected) {
        long before = drops.drops(expected);

        assertThat(decoder.decode(datagram, handler)).isFalse();

        assertThat(drops.drops(expected)).isEqualTo(before + 1);
        assertThat(handler.received).isEmpty();
    }

    // ------------------------------------------------- peeking before verifying

    @Test
    void the_application_name_can_be_read_before_the_tag_is_verified() {
        MessageEncoder encoder = new MessageEncoder("orders", 1,
                MessageAuthenticator.hmacSha256("s3cret".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer datagram = encoder.requestVote(
                ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES), 1L, false);

        assertThat(MessageDecoder.peekApplicationName(datagram))
                .as("the witness server has to route before it knows which key to check with,"
                        + " because the key it needs is chosen by the name it is reading")
                .isEqualTo("orders");
    }

    @Test
    void peeking_a_forged_name_still_cannot_get_a_datagram_accepted() {
        MessageEncoder wrongKey = new MessageEncoder("orders", 1,
                MessageAuthenticator.hmacSha256("not-the-secret".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer datagram = wrongKey.requestVote(
                ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES), 1L, false);

        assertThat(MessageDecoder.peekApplicationName(datagram)).isEqualTo("orders");

        RecordingHandler handler = new RecordingHandler();
        RecordingStats drops = new RecordingStats();
        boolean accepted = new MessageDecoder("orders",
                MessageAuthenticator.hmacSha256("s3cret".getBytes(StandardCharsets.UTF_8)),
                id -> true, drops).decode(datagram, handler);

        assertThat(accepted)
                .as("naming an application is free; producing a tag its key accepts is not")
                .isFalse();
        assertThat(drops.drops(DropReason.BAD_TAG)).isEqualTo(1);
    }

    @Test
    void a_datagram_too_short_to_carry_a_name_peeks_as_nothing() {
        assertThat(MessageDecoder.peekApplicationName(ByteBuffer.allocate(8))).isNull();
    }

    @Test
    void the_message_type_can_be_read_before_the_tag_is_verified() {
        MessageEncoder encoder = new MessageEncoder("orders", 1,
                MessageAuthenticator.hmacSha256("s3cret".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

        assertThat(MessageDecoder.peekMessageType(encoder.nodeConfig(buffer, 1L, 50, 300, 600, 1000, 3)))
                .as("a witness decides whether a name may create an application from the type"
                        + " claimed, before it knows the key that would prove it")
                .isEqualTo(MessageType.NODE_CONFIG);
        assertThat(MessageDecoder.peekMessageType(encoder.requestVote(buffer, 1L, false)))
                .isEqualTo(MessageType.REQUEST_VOTE);
    }

    @Test
    void a_datagram_too_short_or_naming_no_known_type_peeks_as_nothing() {
        assertThat(MessageDecoder.peekMessageType(ByteBuffer.allocate(8))).isNull();

        ByteBuffer unknown = ByteBuffer.allocate(WireFormat.HEADER_BYTES);
        unknown.put(WireFormat.OFFSET_TYPE, (byte) 0x7F);
        unknown.limit(WireFormat.HEADER_BYTES);
        assertThat(MessageDecoder.peekMessageType(unknown))
                .as("an unknown type must not become a type the caller then switches on")
                .isNull();
    }

    @Test
    void a_name_field_full_of_bytes_no_encoder_would_send_peeks_as_nothing() {
        ByteBuffer datagram = ByteBuffer.allocate(WireFormat.HEADER_BYTES);
        datagram.put(WireFormat.OFFSET_APPLICATION, (byte) 0x01);
        datagram.limit(WireFormat.HEADER_BYTES);

        assertThat(MessageDecoder.peekApplicationName(datagram))
                .as("arbitrary bytes must not reach a log line or a map lookup")
                .isNull();
    }
}
