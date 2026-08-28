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
package org.lolaf.primos.core;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;

import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;

/**
 * Constructs nodes.
 *
 * <p>The seam between the public builder and the implementation, so the algorithm stays out of the
 * API package and the API package stays out of the algorithm.
 */
public final class PrimosNodes {

    private PrimosNodes() {
    }

    /** Builds a node. Called by the public builder, which has already validated the configuration. */
    public static PrimosNode create(ClusterConfig config,
                                    StateStore store,
                                    Transport transport,
                                    MonotonicClock clock,
                                    Random random,
                                    byte[] secret,
                                    List<PrimosListener> listeners,
                                    Executor listenerExecutor,
                                    Duration slowCallbackThreshold,
                                    Duration stopTimeout) {
        MessageAuthenticator authenticator = secret == null
                ? MessageAuthenticator.disabled()
                : MessageAuthenticator.hmacSha256(secret);

        RunningPrimosNode node = new RunningPrimosNode(config, store, transport, clock, random,
                authenticator, listenerExecutor, slowCallbackThreshold.toNanos(),
                stopTimeout.toNanos());
        for (PrimosListener listener : listeners) {
            node.addListener(listener);
        }
        return node;
    }
}
