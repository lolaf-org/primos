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
package org.lolaf.primos.transport;

import java.nio.ByteBuffer;

/**
 * Receives whole datagrams from a {@link Transport}.
 *
 * <p>The buffer belongs to the transport and is valid only for the duration of the call. Anything
 * worth keeping must be copied out — which nothing does, because the decoder reads the fields it
 * needs and hands them on as primitives.
 */
@FunctionalInterface
public interface DatagramHandler {

    /**
     * One datagram, positioned at zero and limited to its length.
     */
    void onDatagram(ByteBuffer datagram);
}
