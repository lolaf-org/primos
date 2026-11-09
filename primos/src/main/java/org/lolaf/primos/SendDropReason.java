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
 * Why a datagram never reached the network.
 *
 * <p>The outbound twin of {@link DropReason}, and it covers only what never left: whatever the network loses after a
 * transport has handed a datagram over is invisible here, exactly as it is to a UDP socket.
 */
public enum SendDropReason {

    /**
     * The socket refused the datagram, or took only part of it. Routine on UDP; a rising rate is not.
     */
    SOCKET_ERROR,

    /**
     * Sent after the transport closed. Expected during shutdown, and harmless.
     */
    TRANSPORT_CLOSED,

    /**
     * The transport holds no address for that peer. Should stay at zero: it means something asked for a node that was
     * never configured or, on a witness, for one that has never spoken to it.
     */
    NO_ADDRESS
}
