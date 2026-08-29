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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executor;

import lombok.Setter;

import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;

/**
 * Builds a {@link PrimosNode}.
 *
 * <p>Two objects rather than one long chain, because they answer different questions. The
 * {@link ClusterConfig} says who is in the cluster and is the same shape on every node; this
 * builder wires up one node's runtime around it.
 *
 * <pre>
 * ClusterConfig cluster = ClusterConfig.builder()
 *         .applicationName("orders")
 *         .nodeId(1)
 *         .peers(Peer.application(2, "host-b", 7000))
 *         .witness(Peer.witness(9, "referee", 7000))
 *         .build();
 *
 * PrimosNode node = PrimosNode.builder()
 *         .clusterConfig(cluster)
 *         .transport(transport)
 *         .store(store)
 *         .listener(myListener)
 *         .build();
 * </pre>
 */
public final class PrimosNodeBuilder {

    private final List<PrimosListener> listeners = new ArrayList<>();

    /**
     * Who is in the cluster: the application name, this node's identity, its peers and any
     * witness. Required.
     *
     * <p>A built {@link ClusterConfig} rather than a set of fields proxied through this builder,
     * because the two halves are genuinely different questions. That one describes the cluster and
     * is the same shape on every node; this one wires up one node's runtime — where its state
     * lives, how it reaches the network, who listens. Keeping them apart also means there is one
     * place that validates a cluster's shape rather than two ways to express it.
     */
    @Setter
    private ClusterConfig clusterConfig;

    /** Where the term and vote are kept. Required. */
    @Setter
    private StateStore store;

    /** How this node reaches its peers. Required. */
    @Setter
    private Transport transport;

    /**
     * Where listener callbacks run. By default they run on the node thread, which means a slow
     * listener stalls consensus; supplying an executor moves them off it without primos creating a
     * thread of its own.
     */
    @Setter
    private Executor listenerExecutor;

    /**
     * How long a listener callback may take before it is warned about. Defaults to half the
     * heartbeat interval, which is the point at which one starts to threaten the next heartbeat.
     */
    @Setter
    private Duration slowCallbackThreshold;

    /** How long {@link PrimosNode#stop()} waits for the node thread. Defaults to five seconds. */
    @Setter
    private Duration stopTimeout = Duration.ofSeconds(5);

    /** The clock. Replaced only by tests; production always uses the system's monotonic clock. */
    @Setter
    private MonotonicClock clock = MonotonicClock.SYSTEM;

    /** The source of election timeout randomness. Seed it to make a test reproducible. */
    @Setter
    private Random random = new Random();

    private byte[] secret;

    PrimosNodeBuilder() {
    }

    /** Registered before the node starts, so it cannot miss the first transition. */
    public PrimosNodeBuilder listener(PrimosListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
        return this;
    }

    /**
     * The shared cluster secret. With one set, every datagram carries an HMAC-SHA256 tag and
     * anything unsigned or wrongly signed is dropped before it is parsed.
     *
     * <p>Strongly recommended anywhere that is not a trusted private network: without it, a forged
     * reply is a route to a leader that no majority elected, not merely a nuisance.
     *
     * <p>Copied on the way in, so the caller's array remains theirs to clear.
     */
    public PrimosNodeBuilder clusterSecret(byte[] secret) {
        this.secret = secret == null ? null : secret.clone();
        return this;
    }

    /**
     * Validates everything and builds the node. Nothing is bound and no thread exists until
     * {@link PrimosNode#start()}.
     *
     * <p>Every required value is checked here rather than in its setter, so that a half-configured
     * builder fails in one place with one story, instead of at whichever setter happened to be
     * called first.
     *
     * @throws IllegalStateException if the cluster, the store or the transport is missing
     */
    public PrimosNode build() {
        if (clusterConfig == null) {
            throw new IllegalStateException("a cluster configuration is required:"
                    + " build one with ClusterConfig.builder()");
        }
        if (store == null) {
            throw new IllegalStateException("a state store is required: primos must be able to"
                    + " remember its term and vote across a restart, or it can elect two leaders");
        }
        if (transport == null) {
            throw new IllegalStateException("a transport is required");
        }
        Objects.requireNonNull(stopTimeout, "stopTimeout");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(random, "random");

        Duration slowCallback = slowCallbackThreshold != null
                ? slowCallbackThreshold
                : clusterConfig.timings().heartbeatInterval().dividedBy(2);

        MessageAuthenticator authenticator = secret == null
                ? MessageAuthenticator.disabled()
                : MessageAuthenticator.hmacSha256(secret);

        RunningPrimosNode node = new RunningPrimosNode(clusterConfig, store, transport, clock,
                random, authenticator, listenerExecutor, slowCallback.toNanos(),
                stopTimeout.toNanos());
        for (PrimosListener listener : listeners) {
            // Added before the node starts, so none of them can miss the first transition.
            node.addListener(listener);
        }
        return node;
    }
}
