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

/**
 * The six messages, and the fixed size of each.
 *
 * <p>{@code REQUEST_VOTE} and {@code APPEND_ENTRIES} carry no payload at all: the term and the
 * sender live in the header, which is everything they need. That is also why there is no
 * {@code candidateId} or {@code leaderId} field — in both cases that field simply is the sender.
 */
public enum MessageType {

    /**
     * Asking for a vote. With {@link WireFormat#FLAG_PRE_VOTE} set, asking whether one would be given.
     */
    REQUEST_VOTE(0x01, 0),

    /**
     * Granting or refusing a vote.
     */
    REQUEST_VOTE_REPLY(0x02, 1),

    /**
     * The heartbeat. It never carries entries, because there is no log.
     */
    APPEND_ENTRIES(0x03, 0),

    /**
     * Acknowledging a heartbeat, and carrying the responder's term.
     */
    APPEND_ENTRIES_REPLY(0x04, 1),

    /**
     * A reachability probe, carrying a sequence number.
     */
    PING(0x05, 8),

    /**
     * A probe reply, echoing the sequence number so a round trip can be measured.
     */
    PING_REPLY(0x06, 8),

    /**
     * A departing leader telling one follower to campaign now, rather than after a timeout.
     *
     * <p>Pure header: the sender is the leader handing over and the term is the one it holds,
     * which is everything the target needs to decide whether the instruction is current.
     */
    TIMEOUT_NOW(0x07, 0),

    /**
     * A node telling its peers it is shutting down, so they need not wait out a probe timeout.
     *
     * <p>Pure header, and purely a reachability hint: it says "stop expecting answers from me",
     * never "change the voter set". See {@code ElectionNode#onLeaving}.
     */
    LEAVING(0x08, 0),

    /**
     * A node telling its peers which intervals it is running.
     *
     * <p>Five {@code u32} millisecond values: heartbeat, the two election timeout bounds, the probe
     * interval and the failure threshold. Purely diagnostic — a receiver compares them with its own
     * and says so when they differ, and <strong>changes no decision whatever it finds</strong>. A
     * mismatch that refused a vote would turn a rolling timing change into a partition, which is a
     * worse failure than the silent slowness it detects.
     *
     * <p>Values rather than a hash, because "peer 2 has heartbeat=200ms, we have 50ms" is the whole
     * diagnosis in one line, and a hash could only say that something differs. The same trade the
     * application name makes by travelling readable rather than hashed.
     */
    TIMINGS(0x09, 20);

    private static final MessageType[] BY_CODE = new MessageType[256];

    static {
        for (MessageType type : values()) {
            BY_CODE[type.code] = type;
        }
    }

    /**
     * The byte written at {@link WireFormat#OFFSET_TYPE}. Never zero, so a zeroed buffer is not a
     * message.
     */
    @Getter
    private final int code;

    /**
     * Bytes after the header.
     */
    @Getter
    private final int payloadBytes;

    /**
     * Header plus payload, excluding any authentication tag.
     */
    @Getter
    private final int messageBytes;

    MessageType(int code, int payloadBytes) {
        this.code = code;
        this.payloadBytes = payloadBytes;
        this.messageBytes = WireFormat.HEADER_BYTES + payloadBytes;
    }

    /**
     * The type for a wire code, or null if this build does not know it. Allocation-free: the
     * lookup is a preallocated array rather than {@code values()}, which copies on every call.
     */
    public static MessageType forCode(int code) {
        return (code >= 0 && code < BY_CODE.length) ? BY_CODE[code] : null;
    }

}
