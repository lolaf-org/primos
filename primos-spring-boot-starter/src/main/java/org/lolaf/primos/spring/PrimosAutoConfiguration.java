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
package org.lolaf.primos.spring;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.Timings;
import org.lolaf.primos.spring.PrimosProperties.PeerProperties;
import org.lolaf.primos.store.FileStateStore;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.transport.UdpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;

/**
 * Configures a {@link PrimosNode} from {@code primos.*} and runs it with the context.
 *
 * <p>Inactive until {@code primos.application-name} is set, so the module can sit on a classpath
 * doing nothing. Every bean it contributes backs off if the application defines its own, which is
 * the escape hatch for anything this configuration does not express — custom {@link Timings}, a
 * different {@link StateStore}, a {@link Transport} built by hand.
 */
@AutoConfiguration
@ConditionalOnClass(PrimosNode.class)
@ConditionalOnProperty(prefix = "primos", name = "application-name")
@EnableConfigurationProperties(PrimosProperties.class)
@Slf4j
public class PrimosAutoConfiguration {

    /**
     * The intervals governing elections and failure detection, from a named profile or from values
     * spelled out — and never from both.
     *
     * <p>Assembled here rather than validated during binding, because a failure at bind time is
     * wrapped in a {@code BindException} that buries the reason. This way an operator gets the
     * sentence that was written for them.
     *
     * <p><strong>Keep it uniform across the cluster, witness included.</strong> A node refuses a
     * pre-vote while it has heard from a leader within {@code electionTimeoutMin}, so a member with
     * a longer one is proportionally more reluctant to agree to the failover its vote is needed
     * for. Nothing is corrupted by a mismatch; failover just gets slower in a way that is very hard
     * to see from either side.
     */
    @Bean
    @ConditionalOnMissingBean
    public Timings primosTimings(PrimosProperties properties) {
        return toTimings(properties.timings());
    }

    /**
     * Who is in the cluster. Static configuration: peers are never added or removed at runtime, and
     * quorum is computed from this set rather than from who happens to be reachable.
     */
    @Bean
    @ConditionalOnMissingBean
    public ClusterConfig primosClusterConfig(PrimosProperties properties, Timings timings) {
        ClusterConfig.Builder cluster = ClusterConfig.builder()
                .applicationName(properties.applicationName())
                .nodeId(properties.nodeId())
                .timings(timings)
                .voteLogging(properties.voteLogging())
                .peers(toPeers(properties.peers(), false));
        if (properties.witness() != null) {
            cluster.witness(toPeer(properties.witness(), true));
        }
        return cluster.build();
    }

    /**
     * Where the term and vote are kept.
     *
     * <p>The bean is closed by Spring rather than only by the node. The node closes its store when
     * it stops, but {@code stop()} on a node that was never started is a no-op — so under
     * {@code primos.lifecycle=manual} an application that never starts one would otherwise hold the
     * state directory's exclusive lock until the JVM exited. Both closes are idempotent, so the
     * ordinary path closing twice costs nothing.
     */
    @Bean
    @ConditionalOnMissingBean(name = "primosStateStores")
    public Supplier<StateStore> primosStateStores(PrimosProperties properties) {
        if (properties.stateDirectory() == null) {
            throw new IllegalStateException("primos.state-directory is required: primos must be"
                    + " able to remember its term and vote across a restart, or it can elect two"
                    + " leaders. There is deliberately no default, because one under a temp"
                    + " directory would be silently unsafe after a reboot.");
        }
        return () -> new FileStateStore(properties.stateDirectory());
    }

    /**
     * How this node reaches its peers.
     *
     * <p>Closed by Spring for the same reason as the store: the socket is bound when the bean is
     * built, and a node that is never started never closes it.
     */
    @Bean
    @ConditionalOnMissingBean(name = "primosTransports")
    public Supplier<Transport> primosTransports(PrimosProperties properties, ClusterConfig cluster) {
        byte[] secret = secretBytes(properties);
        // The builder is itself the supplier, so each start binds a fresh socket.
        return UdpTransport.builder()
                .bindAddress(properties.bindAddress() == null
                        ? new InetSocketAddress(properties.bindPort())
                        : new InetSocketAddress(properties.bindAddress(), properties.bindPort()))
                .peers(cluster.allPeers())
                .clusterName(properties.applicationName())
                // Learning a peer's address from its traffic is safe only when a forged datagram
                // cannot claim to be that peer, so it is gated on the secret.
                .addressLearning(secret != null);
    }

    /**
     * The node itself, built and not started. Starting is {@link PrimosNodeLifecycle}'s job, in
     * whichever of the two modes is configured, and stopping is its job in both.
     */
    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean
    public PrimosNode primosNode(PrimosProperties properties,
                                 ClusterConfig cluster,
                                 Supplier<StateStore> stores,
                                 Supplier<Transport> transports,
                                 ObjectProvider<PrimosListener> listeners) {
        var builder = PrimosNode.builder()
                .clusterConfig(cluster)
                .store(stores)
                .transport(transports)
                // Callbacks off the node thread. Every @EventListener in the application hangs off
                // this, and a listener that blocks on the node thread stalls consensus, misses
                // heartbeats and triggers an election that loses the leadership it was watching.
                .listenerExecutor(listenerExecutor(properties));
        byte[] secret = secretBytes(properties);
        if (secret != null) {
            builder.clusterSecret(secret);
        } else {
            log.warn("cluster '{}': no primos.secret is set, so datagrams are neither authenticated"
                    + " nor verified. A forged reply is then a route to a leader that no majority"
                    + " elected, not merely a nuisance. Set one for anything that is not a trusted"
                    + " private network.", properties.applicationName());
        }
        if (properties.macAlgorithm() != null) {
            builder.macAlgorithm(properties.macAlgorithm());
        }
        listeners.orderedStream().forEach(builder::listener);
        return builder.build();
    }

    /**
     * The controller is passed in rather than looked up, so the order that matters — quiesce the
     * gated beans, then start the node — is a direct call that cannot be undone by someone
     * reordering event listeners.
     */
    @Bean
    public PrimosNodeLifecycle primosNodeLifecycle(PrimosNode node,
                                                   PrimosProperties properties,
                                                   ObjectProvider<LeaderLifecycleController> gating) {
        return new PrimosNodeLifecycle(node, properties.lifecycle(),
                properties.leaderLifecycle().enabled(), gating.getIfAvailable());
    }

    @Bean
    public PrimosListener primosEventPublisher(ApplicationEventPublisher events) {
        return new PrimosEventPublisher(events);
    }

    /**
     * The leadership-gated lifecycle, only when it is asked for.
     */
    @Bean
    @ConditionalOnProperty(prefix = "primos.leader-lifecycle", name = "enabled", havingValue = "true")
    public LeaderLifecycleController primosLeaderLifecycleController(ListableBeanFactory beans,
                                                                     PrimosProperties properties) {
        return new LeaderLifecycleController(beans, properties.leaderLifecycle());
    }

    /**
     * The health indicator, only when the application actually has actuator health present.
     */
    @Bean
    @ConditionalOnClass(name = "org.springframework.boot.health.contributor.HealthIndicator")
    @ConditionalOnMissingBean
    public PrimosHealthIndicator primosHealthIndicator(PrimosNode node) {
        return new PrimosHealthIndicator(node);
    }

    private static Timings toTimings(PrimosProperties.TimingsProperties timings) {
        List<String> explicit = timings.explicitKeys();
        if (timings.profile() != null && !explicit.isEmpty()) {
            throw new IllegalStateException("primos.timings.profile is set to '"
                    + timings.profile().name().toLowerCase(Locale.ROOT) + "' and so is "
                    + String.join(", ", explicit) + ". Choose one: a profile is a set of intervals"
                    + " picked to hold together, so overriding part of one leaves a custom set"
                    + " whose author has not seen the other values. Either drop the profile and"
                    + " name every interval you want, or drop the individual keys.");
        }
        if (timings.profile() != null) {
            return timings.profile() == PrimosProperties.Profile.WAN ? Timings.wan() : Timings.lan();
        }
        if (explicit.isEmpty()) {
            return Timings.lan();
        }
        // The two bounds are a pair: setting one against the other's default is how an invalid
        // range gets built. Timings rejects that at build(), but naming the property is a better
        // error than naming a Java field, so it is caught here first.
        boolean hasMin = timings.electionTimeoutMin() != null;
        boolean hasMax = timings.electionTimeoutMax() != null;
        if (hasMin != hasMax) {
            throw new IllegalStateException("primos.timings.election-timeout-"
                    + (hasMin ? "min" : "max") + " is set without its pair. The election timeout is"
                    + " a range drawn from afresh on every reset, and setting one end against the"
                    + " other's default is how an invalid or too-narrow range is built. Set both.");
        }
        Timings.Builder builder = Timings.builder();
        if (timings.heartbeatInterval() != null) {
            builder.heartbeatInterval(timings.heartbeatInterval());
        }
        if (hasMin) {
            builder.electionTimeoutMin(timings.electionTimeoutMin());
            builder.electionTimeoutMax(timings.electionTimeoutMax());
        }
        if (timings.probeInterval() != null) {
            builder.probeInterval(timings.probeInterval());
        }
        if (timings.advertisementInterval() != null) {
            builder.advertisementInterval(timings.advertisementInterval());
        }
        if (timings.failureThreshold() != null) {
            builder.failureThreshold(timings.failureThreshold());
        }
        // Every relationship between these — the election timeout above five heartbeats, the range
        // wider than one heartbeat, the probe ceiling — is checked in here, with its own message.
        return builder.build();
    }

    private static Executor listenerExecutor(PrimosProperties properties) {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor(
                "primos-" + properties.applicationName() + "-events-");
        // One at a time, so events reach the application in the order the node produced them.
        executor.setConcurrencyLimit(1);
        return executor;
    }

    private static byte[] secretBytes(PrimosProperties properties) {
        String secret = properties.secret();
        return secret == null || secret.isEmpty()
                ? null : secret.getBytes(StandardCharsets.UTF_8);
    }

    private static List<Peer> toPeers(List<PeerProperties> peers, boolean witness) {
        List<Peer> result = new ArrayList<>();
        peers.forEach(peer -> result.add(toPeer(peer, witness)));
        return result;
    }

    /**
     * The name is passed through untouched, including when it is null.
     *
     * <p>{@link Peer} already turns a missing name into {@code node-<id>} or {@code witness-<id>},
     * and substituting anything here — the bare id, say — would give a peer configured through
     * Spring a different default from the identical peer configured through the library.
     */
    private static Peer toPeer(PeerProperties peer, boolean witness) {
        return witness
                ? Peer.witness(peer.id(), peer.name(), peer.host(), peer.port())
                : Peer.application(peer.id(), peer.name(), peer.host(), peer.port());
    }
}
