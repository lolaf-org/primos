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
package org.lolaf.primos;

/**
 * Why a received datagram was discarded.
 *
 * <p>One constant per check in the receive pipeline, counted separately. A cluster that is
 * silently dropping everything must be diagnosable from a metric rather than from a packet
 * capture, and the reasons distinguish a misconfiguration from an attack: a stream of
 * {@link #WRONG_APPLICATION} is a neighbouring cluster, while {@link #BAD_TAG} is either a wrong
 * secret or someone forging traffic.
 */
public enum DropReason {

    /** Too short to contain a header, or longer than a single unfragmented datagram. */
    MALFORMED,

    /** Authentication is enabled and the HMAC tag did not verify over the datagram. */
    BAD_TAG,

    /** The leading magic bytes were not primos'. Almost always unrelated traffic on the port. */
    BAD_MAGIC,

    /** A protocol version this build does not implement. Never interpreted, only counted. */
    UNKNOWN_VERSION,

    /** Addressed to a different clustered application. Usually a neighbouring cluster or a typo. */
    WRONG_APPLICATION,

    /** A message type this build does not know, or the wrong length for the type claimed. */
    UNKNOWN_TYPE,

    /** The sender id is not a configured peer of this cluster. */
    UNKNOWN_PEER
}
