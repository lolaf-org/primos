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

import lombok.Setter;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

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
 *         .peer(Peer.application(2, "host-b", 7000))
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

    /**
     * Makes the store the node keeps its term and vote in. Required. See {@link #store(Supplier)}.
     */
    private Supplier<StateStore> stores;

    /**
     * Makes the transport the node reaches its peers through. Required.
     */
    private Supplier<Transport> transports;

    /**
     * Where the counters go. One sink, fixed here, because some of these fire per datagram — see
     * {@link PrimosStatsListener} for why there is no list and no executor.
     */
    @Setter
    private PrimosStatsListener statsListener = PrimosStatsListener.NONE;

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

    /**
     * How long {@link PrimosNode#stop()} waits for the node thread. Defaults to five seconds.
     */
    @Setter
    private Duration stopTimeout = Duration.ofSeconds(5);

    /**
     * The clock. Replaced only by tests; production always uses the system's monotonic clock.
     */
    @Setter
    private MonotonicClock clock = MonotonicClock.SYSTEM;

    /**
     * The source of election timeout randomness. Seed it to make a test reproducible.
     */
    @Setter
    private Random random = new SecureRandom();

    /**
     * The shared cluster secret. With one set, every datagram carries an HMAC-SHA256 tag and
     * anything unsigned or wrongly signed is dropped before it is parsed.
     *
     * <p>Strongly recommended anywhere that is not a trusted private network: without it, a forged
     * reply is a route to a leader that no majority elected, not merely a nuisance.
     *
     * <p>Copied in {@link #build()}, so the caller's array remains theirs to clear.
     */
    @Setter
    private byte[] clusterSecret;

    /**
     * Which {@link javax.crypto.Mac} algorithm signs the datagrams. Defaults to HMAC-SHA256, which
     * is the right answer for almost everyone; the setting exists for a deployment whose policy
     * names something else, and for the day SHA-256 stops being above suspicion.
     *
     * <p>Nothing on the wire says which algorithm was used, so this is local policy that every node
     * of the cluster and its witness must share. Disagreeing about it looks exactly like
     * disagreeing about the secret: every datagram fails verification and is counted under
     * {@link org.lolaf.primos.DropReason#BAD_TAG}.
     */
    @Setter
    private String macAlgorithm = MessageAuthenticator.DEFAULT_ALGORITHM;

    PrimosNodeBuilder() {
    }

    /**
     * How to make the store this node keeps its term and vote in. Required.
     */
    public PrimosNodeBuilder store(Supplier<StateStore> stores) {
        this.stores = Objects.requireNonNull(stores, "stores");
        return this;
    }

    /**
     * How to make the transport this node reaches its peers through. Required.
     */
    public PrimosNodeBuilder transport(Supplier<Transport> transports) {
        this.transports = Objects.requireNonNull(transports, "transports");
        return this;
    }

    public PrimosNodeBuilder listener(PrimosListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
        return this;
    }

    /**
     * Validates everything and builds the node.
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
        if (stores == null) {
            throw new IllegalStateException("a state store is required: primos must be able to"
                    + " remember its term and vote across a restart, or it can elect two leaders");
        }
        if (transports == null) {
            throw new IllegalStateException("a transport is required");
        }
        Objects.requireNonNull(macAlgorithm, "macAlgorithm");
        Objects.requireNonNull(stopTimeout, "stopTimeout");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(random, "random");

        Duration slowCallback = slowCallbackThreshold != null
                ? slowCallbackThreshold
                : clusterConfig.timings().heartbeatInterval().dividedBy(2);

        MessageAuthenticator authenticator = clusterSecret == null
                ? MessageAuthenticator.disabled()
                // Copied at the boundary rather than aliasing the caller's array, so a caller
                // that clears its own secret after building cannot blank this node's key.
                // SecretKeySpec copies too, but that is the JCE's business; the promise on the
                // setter is ours.
                : MessageAuthenticator.hmac(macAlgorithm, clusterSecret.clone());

        PrimosNodeImpl node = new PrimosNodeImpl(clusterConfig, stores, transports, clock,
                random, authenticator, listenerExecutor, slowCallback.toNanos(),
                stopTimeout.toNanos(),
                // Guarded here rather than inside the algorithm: a metrics binder that throws must
                // not reach the node's catch-all and cost the cluster a voter.
                PrimosStatsListener.guarded(statsListener, clusterConfig.applicationName(),
                        clusterConfig.nodeId()));
        for (PrimosListener listener : listeners) {
            // Added before the node starts, so none of them can miss the first transition.
            node.addListener(listener);
        }
        return node;
    }
}
