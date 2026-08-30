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

import java.io.IOException;
import java.net.DatagramSocket;

/**
 * A port the operating system has just said is free.
 *
 * <p>Every real-socket test needs one and none of them may hard-code one, because two tests
 * sharing a port fail as a flake in whichever ran second. Shared rather than copied per test
 * class, and published in the test-jar so the witness module's tests use the same one.
 */
public final class Ports {

    private Ports() {
    }

    /**
     * An ephemeral port, released again before this returns.
     *
     * <p>Inherently a race: the port is free at the instant it is reported and something else may
     * take it before the caller binds. Nothing better exists without keeping the socket, and
     * binding to port 0 directly — which most tests here do — avoids the question entirely. Use
     * this only where the port has to be known before the thing that binds it is built.
     */
    public static int free() {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("no free port", e);
        }
    }
}
