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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Byte-for-byte encodings, frozen.
 *
 * <p>These vectors were written from the layout in the specification rather than captured from
 * this code, so they check the implementation against the design rather than against itself. Any
 * accidental change to an offset, a width or the byte order fails here, loudly, instead of becoming
 * a mystery when two versions of primos meet on a network.
 *
 * <p>All of them encode application {@code orders}, sender {@code 3}, term {@code 42} and, where
 * there is one, sequence {@code 7}.
 */
class GoldenVectorsTest {

    private static final String REQUEST_VOTE =
            "50524d53010100006f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a00000003";

    private static final String REQUEST_VOTE_PRE =
            "50524d53010100016f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a00000003";

    private static final String REQUEST_VOTE_REPLY =
            "50524d53010200016f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a0000000301";

    private static final String APPEND_ENTRIES =
            "50524d53010300006f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a00000003";

    private static final String APPEND_ENTRIES_REPLY =
            "50524d53010400006f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a0000000301";

    private static final String PING =
            "50524d53010500006f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a000000030000000000000007";

    private static final String PING_REPLY =
            "50524d53010600006f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a000000030000000000000007";

    /**
     * Header, then heartbeat 50ms, election 300-600ms, probe 1000ms, failure threshold 3 — the LAN
     * profile, as five big-endian {@code u32}s.
     */
    private static final String TIMINGS =
            "50524d53010900006f7264657273000000000000000000000000000000000000"
                    + "0000000000000000000000000000002a00000003"
                    + "000000320000012c00000258000003e800000003";

    private final MessageEncoder encoder =
            new MessageEncoder("orders", 3, MessageAuthenticator.disabled());
    private final ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    private static String hex(ByteBuffer buffer) {
        StringBuilder out = new StringBuilder(buffer.remaining() * 2);
        for (int i = buffer.position(); i < buffer.limit(); i++) {
            out.append(String.format("%02x", buffer.get(i)));
        }
        return out.toString();
    }

    private static int differingBytes(String left, String right) {
        assertThat(left).hasSameSizeAs(right);
        int differences = 0;
        for (int i = 0; i < left.length(); i += 2) {
            if (!left.regionMatches(i, right, i, 2)) {
                differences++;
            }
        }
        return differences;
    }

    @Test
    void request_vote_is_pure_header() {
        assertThat(hex(encoder.requestVote(buffer, 42, false))).isEqualTo(REQUEST_VOTE);
    }

    @Test
    void a_pre_vote_differs_from_a_vote_in_exactly_one_byte() {
        String preVote = hex(encoder.requestVote(buffer, 42, true));

        assertThat(preVote).isEqualTo(REQUEST_VOTE_PRE);
        assertThat(differingBytes(preVote, REQUEST_VOTE))
                .as("a pre-vote is one flag bit, not a separate message type")
                .isEqualTo(1);
    }

    @Test
    void request_vote_reply_appends_one_byte() {
        assertThat(hex(encoder.requestVoteReply(buffer, 42, true, true)))
                .isEqualTo(REQUEST_VOTE_REPLY);
    }

    @Test
    void append_entries_is_pure_header_and_never_carries_entries() {
        assertThat(hex(encoder.appendEntries(buffer, 42))).isEqualTo(APPEND_ENTRIES);
    }

    @Test
    void append_entries_reply_appends_one_byte() {
        assertThat(hex(encoder.appendEntriesReply(buffer, 42, true)))
                .isEqualTo(APPEND_ENTRIES_REPLY);
    }

    @Test
    void ping_appends_a_big_endian_sequence() {
        assertThat(hex(encoder.ping(buffer, 42, 7))).isEqualTo(PING);
    }

    @Test
    void ping_reply_differs_from_ping_in_exactly_its_type_byte() {
        assertThat(hex(encoder.pingReply(buffer, 42, 7))).isEqualTo(PING_REPLY);
        assertThat(differingBytes(PING, PING_REPLY)).isEqualTo(1);
    }

    @Test
    void timings_appends_five_big_endian_millisecond_values() {
        assertThat(hex(encoder.timings(buffer, 42, 50, 300, 600, 1000, 3))).isEqualTo(TIMINGS);
    }

    @Test
    void the_application_name_is_zero_padded_to_a_fixed_width_field() {
        String encoded = hex(encoder.appendEntries(buffer, 42));
        String field = encoded.substring(WireFormat.OFFSET_APPLICATION * 2,
                (WireFormat.OFFSET_APPLICATION + WireFormat.APPLICATION_BYTES) * 2);

        assertThat(field).startsWith("6f7264657273").endsWith("0000000000")
                .hasSize(WireFormat.APPLICATION_BYTES * 2);
    }

    @Test
    void every_message_is_the_size_the_specification_says() {
        assertThat(MessageType.REQUEST_VOTE.messageBytes()).isEqualTo(52);
        assertThat(MessageType.REQUEST_VOTE_REPLY.messageBytes()).isEqualTo(53);
        assertThat(MessageType.APPEND_ENTRIES.messageBytes()).isEqualTo(52);
        assertThat(MessageType.APPEND_ENTRIES_REPLY.messageBytes()).isEqualTo(53);
        assertThat(MessageType.PING.messageBytes()).isEqualTo(60);
        assertThat(MessageType.PING_REPLY.messageBytes()).isEqualTo(60);
        assertThat(MessageType.TIMEOUT_NOW.messageBytes()).isEqualTo(52);
        assertThat(MessageType.LEAVING.messageBytes()).isEqualTo(52);
        assertThat(MessageType.TIMINGS.messageBytes()).isEqualTo(72);
        assertThat(WireFormat.HEADER_BYTES).isEqualTo(52);
    }

    @Test
    void the_largest_authenticated_message_leaves_the_datagram_budget_untroubled() {
        // TIMINGS took this title from PING when it was added: five u32s is a bigger payload than
        // one u64. Still less than a quarter of the budget, and still fixed.
        int largest = MessageType.TIMINGS.messageBytes() + WireFormat.DEFAULT_TAG_BYTES;

        assertThat(largest).isEqualTo(104).isLessThan(WireFormat.MAX_DATAGRAM_BYTES);
        for (MessageType type : MessageType.values()) {
            assertThat(type.messageBytes() + WireFormat.DEFAULT_TAG_BYTES)
                    .as("%s must not exceed the largest", type)
                    .isLessThanOrEqualTo(largest);
        }
    }

    @Test
    void zero_is_never_a_valid_type_so_a_zeroed_buffer_is_not_a_message() {
        assertThat(MessageType.forCode(0)).isNull();
    }

    @Test
    void the_type_codes_are_the_ones_the_specification_assigns() {
        // Pinned rather than derived: these travel on the wire, so a reordering of the enum must
        // fail here rather than in the field between two versions.
        assertThat(MessageType.forCode(0x01)).isEqualTo(MessageType.REQUEST_VOTE);
        assertThat(MessageType.forCode(0x02)).isEqualTo(MessageType.REQUEST_VOTE_REPLY);
        assertThat(MessageType.forCode(0x03)).isEqualTo(MessageType.APPEND_ENTRIES);
        assertThat(MessageType.forCode(0x04)).isEqualTo(MessageType.APPEND_ENTRIES_REPLY);
        assertThat(MessageType.forCode(0x05)).isEqualTo(MessageType.PING);
        assertThat(MessageType.forCode(0x06)).isEqualTo(MessageType.PING_REPLY);
        assertThat(MessageType.forCode(0x07)).isEqualTo(MessageType.TIMEOUT_NOW);
        assertThat(MessageType.forCode(0x08)).isEqualTo(MessageType.LEAVING);
        assertThat(MessageType.forCode(0x09)).isEqualTo(MessageType.TIMINGS);
        assertThat(MessageType.forCode(0x0A)).isNull();
    }
}
