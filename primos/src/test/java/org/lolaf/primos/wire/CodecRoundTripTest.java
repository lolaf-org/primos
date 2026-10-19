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

import org.junit.jupiter.api.Test;
import org.lolaf.primos.DropReason;
import org.lolaf.primos.RecordingStats;

class CodecRoundTripTest {

    private static final String APP = "orders";
    private static final int SENDER = 3;

    private final RecordingStats drops = new RecordingStats();
    private final RecordingHandler handler = new RecordingHandler();
    private final ByteBuffer buffer = ByteBuffer.allocate(WireFormat.MAX_DATAGRAM_BYTES);

    private MessageEncoder encoder(MessageAuthenticator auth) {
        return new MessageEncoder(APP, SENDER, auth);
    }

    private MessageDecoder decoder(MessageAuthenticator auth) {
        return new MessageDecoder(APP, auth, id -> id == SENDER, drops);
    }

    @Test
    void the_leader_version_survives_a_round_trip_at_full_width() {
        MessageEncoder encoder = encoder(MessageAuthenticator.disabled());

        assertThat(decode(decoder(MessageAuthenticator.disabled()), encoder.appendEntries(buffer, 7, Long.MAX_VALUE)))
                .isEqualTo("appendEntries(3,7,version=" + Long.MAX_VALUE + ")");
    }

    @Test
    void a_heartbeat_without_its_leader_version_is_rejected() {
        ByteBuffer datagram = encoder(MessageAuthenticator.disabled()).appendEntries(buffer, 7, 0);
        datagram.limit(WireFormat.HEADER_BYTES);

        assertThat(decoder(MessageAuthenticator.disabled()).decode(datagram, handler)).isFalse();
        assertThat(drops.drops(DropReason.UNKNOWN_TYPE)).isEqualTo(1);
        assertThat(drops.totalDrops()).isEqualTo(1);
    }

    @Test
    void every_message_survives_a_round_trip_unauthenticated() {
        MessageEncoder encoder = encoder(MessageAuthenticator.disabled());
        MessageDecoder decoder = decoder(MessageAuthenticator.disabled());

        assertThat(decode(decoder, encoder.requestVote(buffer, 7, false)))
                .isEqualTo("requestVote(3,7,preVote=false)");
        assertThat(decode(decoder, encoder.requestVote(buffer, 7, true)))
                .isEqualTo("requestVote(3,7,preVote=true)");
        assertThat(decode(decoder, encoder.requestVoteReply(buffer, 7, true, false)))
                .isEqualTo("requestVoteReply(3,7,granted=true,preVote=false)");
        assertThat(decode(decoder, encoder.requestVoteReply(buffer, 7, false, true)))
                .isEqualTo("requestVoteReply(3,7,granted=false,preVote=true)");
        assertThat(decode(decoder, encoder.appendEntries(buffer, 7, 0)))
                .isEqualTo("appendEntries(3,7,version=0)");
        assertThat(decode(decoder, encoder.appendEntriesReply(buffer, 7, true)))
                .isEqualTo("appendEntriesReply(3,7,success=true)");
        assertThat(decode(decoder, encoder.ping(buffer, 7, 99)))
                .isEqualTo("ping(3,7,seq=99)");
        assertThat(decode(decoder, encoder.pingReply(buffer, 7, 99)))
                .isEqualTo("pingReply(3,7,seq=99)");
        assertThat(decode(decoder, encoder.timeoutNow(buffer, 7)))
                .isEqualTo("timeoutNow(3,7)");
        assertThat(decode(decoder, encoder.leaving(buffer, 7)))
                .isEqualTo("leaving(3,7)");

        assertThat(drops.totalDrops()).isZero();
    }

    @Test
    void every_message_survives_a_round_trip_authenticated() {
        byte[] secret = "a-shared-cluster-secret".getBytes();
        MessageEncoder encoder = encoder(MessageAuthenticator.hmacSha256(secret));
        MessageDecoder decoder = decoder(MessageAuthenticator.hmacSha256(secret));

        assertThat(decode(decoder, encoder.requestVote(buffer, 7, true)))
                .isEqualTo("requestVote(3,7,preVote=true)");
        assertThat(decode(decoder, encoder.ping(buffer, 7, 99))).isEqualTo("ping(3,7,seq=99)");
        assertThat(decode(decoder, encoder.appendEntriesReply(buffer, 7, false)))
                .isEqualTo("appendEntriesReply(3,7,success=false)");
        assertThat(decode(decoder, encoder.timeoutNow(buffer, 7)))
                .isEqualTo("timeoutNow(3,7)");

        assertThat(drops.totalDrops()).isZero();
    }

    @Test
    void a_tag_adds_exactly_thirty_two_bytes() {
        MessageEncoder plain = encoder(MessageAuthenticator.disabled());
        MessageEncoder tagged = encoder(MessageAuthenticator.hmacSha256("secret".getBytes()));

        assertThat(plain.requestVote(buffer, 1, false).remaining()).isEqualTo(52);
        assertThat(tagged.requestVote(buffer, 1, false).remaining()).isEqualTo(52 + 32);
    }

    @Test
    void extreme_terms_and_sequences_survive_intact() {
        MessageEncoder encoder = encoder(MessageAuthenticator.disabled());
        MessageDecoder decoder = decoder(MessageAuthenticator.disabled());

        assertThat(decode(decoder, encoder.ping(buffer, Long.MAX_VALUE, Long.MAX_VALUE)))
                .isEqualTo("ping(3," + Long.MAX_VALUE + ",seq=" + Long.MAX_VALUE + ")");
    }

    @Test
    void the_maximum_node_id_survives_intact() {
        MessageEncoder encoder = new MessageEncoder(APP, 1023, MessageAuthenticator.disabled());
        MessageDecoder decoder = new MessageDecoder(APP, MessageAuthenticator.disabled(),
                id -> id == 1023, drops);

        assertThat(decode(decoder, encoder.appendEntries(buffer, 5, 0)))
                .isEqualTo("appendEntries(1023,5,version=0)");
    }

    @Test
    void a_name_shorter_than_the_field_is_zero_padded_not_truncated() {
        MessageEncoder encoder = new MessageEncoder("a", SENDER, MessageAuthenticator.disabled());
        MessageDecoder decoder = new MessageDecoder("a", MessageAuthenticator.disabled(),
                id -> true, drops);

        assertThat(decode(decoder, encoder.appendEntries(buffer, 1, 0))).isEqualTo("appendEntries(3,1,version=0)");
    }

    @Test
    void a_full_length_name_still_fits_the_field() {
        String name = new String(new char[32]).replace('\0', 'x');
        MessageEncoder encoder = new MessageEncoder(name, SENDER, MessageAuthenticator.disabled());
        MessageDecoder decoder = new MessageDecoder(name, MessageAuthenticator.disabled(),
                id -> true, drops);

        assertThat(decode(decoder, encoder.appendEntries(buffer, 1, 0))).isEqualTo("appendEntries(3,1,version=0)");
    }

    private String decode(MessageDecoder decoder, ByteBuffer datagram) {
        handler.received.clear();
        assertThat(decoder.decode(datagram, handler)).isTrue();
        return handler.only();
    }
}
