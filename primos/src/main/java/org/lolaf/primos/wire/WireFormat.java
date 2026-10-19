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

/**
 * The byte layout every primos datagram follows.
 *
 * <p>Every message type has a size known at compile time. That single property does most of the
 * validation work: one length comparison rejects every truncated, padded or malformed datagram
 * before a field is read, and the parser contains no loops, so there is nothing in it to attack.
 *
 * <p>There is no length prefix and no framing state machine, because UDP gives us the frame — a
 * datagram arrives whole or not at all.
 *
 * <pre>
 * offset  size  field
 *      0     4  magic 'PRMS'
 *      4     1  version
 *      5     1  message type
 *      6     2  flags (bit 0 = pre-vote; the rest reserved, sent zero, ignored on receive)
 *      8    32  application name, UTF-8, zero-padded
 *     40     8  term
 *     48     4  senderId
 *     52     -  payload, per message type
 *      -     -  authentication tag, when enabled: 32 bytes for the default HMAC-SHA256
 * </pre>
 *
 * <p>Big-endian throughout, which is both network byte order and {@link java.nio.ByteBuffer}'s
 * default, so nothing ever calls {@code order()}.
 */
public final class WireFormat {

    /**
     * Makes a capture recognisable at a glance and a heuristic dissector a one-liner.
     */
    public static final byte[] MAGIC = {'P', 'R', 'M', 'S'};

    /**
     * Bumped only for an incompatible layout change. An unknown version is dropped, never guessed.
     */
    public static final byte VERSION = 1;

    public static final int OFFSET_MAGIC = 0;
    public static final int OFFSET_VERSION = 4;
    public static final int OFFSET_TYPE = 5;
    public static final int OFFSET_FLAGS = 6;
    public static final int OFFSET_APPLICATION = 8;
    public static final int OFFSET_TERM = 40;
    public static final int OFFSET_SENDER = 48;

    /**
     * Size of the header carried by all nine messages.
     */
    public static final int HEADER_BYTES = 52;

    /**
     * The application name field. Fixed width rather than length-prefixed so the parse stays a
     * sequence of constant offsets, and stored readable rather than hashed so a drop can say
     * <em>which</em> foreign cluster is talking.
     */
    public static final int APPLICATION_BYTES = 32;

    /**
     * Length of a tag from the default HMAC-SHA256.
     *
     * <p>Not a property of the format: the tag length follows whichever {@code Mac} a cluster is
     * configured with, and everything that measures a datagram asks
     * {@link MessageAuthenticator#tagBytes()} rather than this. It is here because the layout above
     * has to show a number, and this is the one almost every cluster will see.
     */
    public static final int DEFAULT_TAG_BYTES = 32;

    /**
     * The largest datagram primos will send or accept.
     *
     * <p>576 bytes is the smallest IPv4 datagram every host must accept, less 60 of maximum IP
     * header and 8 of UDP header. Staying under it means a primos datagram is never fragmented, so
     * there is no reassembly and no partial-message state anywhere in the transport. The largest
     * message actually defined is 104 bytes, so this is a ceiling with room to spare rather than a
     * constraint anything presses against.
     */
    public static final int MAX_DATAGRAM_BYTES = 508;

    /**
     * Marks a {@code RequestVote} or its reply as a pre-vote, which mutates no persistent state.
     */
    public static final int FLAG_PRE_VOTE = 0x0001;

    private WireFormat() {
    }
}
