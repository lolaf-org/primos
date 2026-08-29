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

import org.lolaf.primos.core.PrimosNodes;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;

/**
 * Builds a {@link PrimosNode}.
 *
 * <p>Hand-written rather than generated, because {@link #build()} has real work to do — the
 * application name has to fit the field it travels in, ids have to be unique, the timings have to
 * hold together — and because this is the most-read piece of API in the library, where the method
 * names and their documentation matter more than the lines they save.
 *
 * <pre>
 * PrimosNode node = PrimosNode.builder()
 *         .applicationName("orders")
 *         .nodeId(1)
 *         .peers(Peer.application(2, "host-b", 7000))
 *         .witness(Peer.witness(9, "referee", 7000))
 *         .transport(transport)
 *         .store(store)
 *         .listener(myListener)
 *         .build();
 * </pre>
 */
public final class PrimosNodeBuilder {

    private final ClusterConfig.Builder cluster = ClusterConfig.builder();
    private final List<PrimosListener> listeners = new ArrayList<>();

    /** Where the term and vote are kept. Required. */
    @Setter
    private StateStore store;

    /** How this node reaches its peers. Required. */
    @Setter
    private Transport transport;

    private MonotonicClock clock = MonotonicClock.SYSTEM;
    private Random random = new Random();
    private byte[] secret;

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

    private Duration stopTimeout = Duration.ofSeconds(5);

    PrimosNodeBuilder() {
    }

    /** Which clustered application this is. Every node of one cluster must agree on it. */
    public PrimosNodeBuilder applicationName(String applicationName) {
        cluster.applicationName(applicationName);
        return this;
    }

    /** This node's id, unique within the application. */
    public PrimosNodeBuilder nodeId(int nodeId) {
        cluster.nodeId(nodeId);
        return this;
    }

    /** A human-readable name for this node, used in logs. Defaults to {@code node-<id>}. */
    public PrimosNodeBuilder nodeName(String nodeName) {
        cluster.nodeName(nodeName);
        return this;
    }

    /** The other application instances. This node must not be among them. */
    public PrimosNodeBuilder peers(Peer... peers) {
        cluster.peers(peers);
        return this;
    }

    /** The other application instances. This node must not be among them. */
    public PrimosNodeBuilder peers(List<Peer> peers) {
        cluster.peers(peers);
        return this;
    }

    /**
     * The witness, at most one: a voter that never leads, in a third failure domain, which is what
     * lets a two-node pair survive a partition.
     */
    public PrimosNodeBuilder witness(Peer witness) {
        cluster.witness(witness);
        return this;
    }

    /** Declares this node to be the witness rather than an application instance. */
    public PrimosNodeBuilder asWitness() {
        cluster.asWitness();
        return this;
    }

    /** Election, heartbeat and probe intervals. Defaults to {@link Timings#lan()}. */
    public PrimosNodeBuilder timings(Timings timings) {
        cluster.timings(timings);
        return this;
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
     */
    public PrimosNodeBuilder clusterSecret(byte[] secret) {
        this.secret = secret == null ? null : secret.clone();
        return this;
    }

    /** How long {@link PrimosNode#stop()} waits for the node thread. Defaults to five seconds. */
    public PrimosNodeBuilder stopTimeout(Duration stopTimeout) {
        this.stopTimeout = Objects.requireNonNull(stopTimeout, "stopTimeout");
        return this;
    }

    /** The clock. Replaced only by tests; production always uses the system's monotonic clock. */
    public PrimosNodeBuilder clock(MonotonicClock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        return this;
    }

    /** The source of election timeout randomness. Seed it to make a test reproducible. */
    public PrimosNodeBuilder random(Random random) {
        this.random = Objects.requireNonNull(random, "random");
        return this;
    }

    /**
     * Validates everything and builds the node. Nothing is bound and no thread exists until
     * {@link PrimosNode#start()}.
     *
     * @throws IllegalArgumentException if the configuration cannot describe a working cluster
     * @throws IllegalStateException if a store or transport is missing
     */
    public PrimosNode build() {
        if (store == null) {
            throw new IllegalStateException("a state store is required: primos must be able to"
                    + " remember its term and vote across a restart, or it can elect two leaders");
        }
        if (transport == null) {
            throw new IllegalStateException("a transport is required");
        }
        ClusterConfig config = cluster.build();
        Duration slowCallback = slowCallbackThreshold != null
                ? slowCallbackThreshold
                : config.timings().heartbeatInterval().dividedBy(2);

        return PrimosNodes.create(config, store, transport, clock, random, secret, listeners,
                listenerExecutor, slowCallback, stopTimeout);
    }
}
