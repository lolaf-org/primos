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
import org.lolaf.primos.*;
import org.lolaf.primos.spring.PrimosProperties.PeerProperties;
import org.lolaf.primos.store.FileStateStore;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.transport.UdpTransport;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * Configures a {@link PrimosNode} from {@code primos.*} and runs it with the context.
 *
 * <p>Inactive until {@code primos.application-name} is set, so the module can sit on a classpath
 * doing nothing. Every bean it contributes backs off if the application defines its own, which is
 * the escape hatch for anything this configuration does not express: custom {@link Timings}, a
 * different {@link StateStore}, a {@link Transport} built by hand.
 */
@AutoConfiguration
@ConditionalOnClass(PrimosNode.class)
@ConditionalOnProperty(prefix = "primos", name = "application-name")
@EnableConfigurationProperties(PrimosProperties.class)
@Slf4j
public class PrimosAutoConfiguration {

    private static Timings toTimings(PrimosProperties.TimingsProperties timings) {
        String prefix = "primos.timings";
        return Timings.resolve(prefix + ".profile", timings.profile() == null ? null : timings.profile().name(),
                timings.explicitKeys(prefix), Timings.lan(),
                () -> Timings.builder()
                        .heartbeatIntervalIfSet(timings.heartbeatInterval())
                        .electionTimeoutIfSet(timings.electionTimeoutMin(), timings.electionTimeoutMax(),
                                prefix + ".election-timeout-min", prefix + ".election-timeout-max")
                        .probeIntervalIfSet(timings.probeInterval())
                        .advertisementIntervalIfSet(timings.advertisementInterval())
                        .failureThresholdIfSet(timings.failureThreshold())
                        .build());
    }

    private static byte[] secretBytes(PrimosProperties properties) {
        String secret = properties.secret();
        return secret == null || secret.isEmpty()
                ? null : secret.getBytes(StandardCharsets.UTF_8);
    }

    private static Peer toPeer(PeerProperties peer, boolean witness) {
        return witness
                ? Peer.witness(peer.id(), peer.name(), peer.host(), peer.port())
                : Peer.application(peer.id(), peer.name(), peer.host(), peer.port());
    }

    /**
     * The intervals governing elections and failure detection, from a named profile or from values
     * spelled out, and never from both.
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
        ClusterConfig.ClusterConfigBuilder cluster = ClusterConfig.builder()
                .applicationName(properties.applicationName())
                .nodeId(properties.nodeId())
                .timings(timings)
                .voteLogging(properties.voteLogging())
                .peers(properties.peers().stream().map(peer -> toPeer(peer, false)).toList());
        if (properties.witness() != null) {
            cluster.witness(toPeer(properties.witness(), true));
        }
        return cluster.build();
    }

    /**
     * Makes the store the term and vote are kept in, one for each start of the node, which closes it.
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
                                 ObjectProvider<PrimosListener> listeners,
                                 ObjectProvider<PrimosStatsListener> statsListeners) {
        var builder = PrimosNode.builder()
                .clusterConfig(cluster)
                .store(stores)
                .transport(transports);
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
        // One sink, fixed here, because some of these fire per datagram. Several beans compose
        // into one rather than becoming a list the node would have to iterate.
        PrimosStatsListener[] sinks = statsListeners.orderedStream().toArray(PrimosStatsListener[]::new);
        if (sinks.length > 0) {
            builder.statsListener(sinks.length == 1 ? sinks[0] : PrimosStatsListener.of(sinks));
        }
        return builder.build();
    }

    /**
     * The controller is passed in rather than looked up, so the order that matters (quiesce the
     * gated beans, then start the node) is a direct call that cannot be undone by someone
     * reordering event listeners.
     */
    @Bean
    public PrimosNodeLifecycle primosNodeLifecycle(PrimosNode node,
                                                   PrimosProperties properties,
                                                   ObjectProvider<LeaderLifecycleController> gating,
                                                   ApplicationContext context) {
        return new PrimosNodeLifecycle(node, properties.lifecycle(), gating.getIfAvailable(), context);
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
     * The health indicator, only when the application actually has actuator health present. A nested class, because
     * Spring reads every method of a configuration class before any condition: a {@code @Bean} method returning a
     * {@code HealthIndicator} here would fail the whole auto-configuration without actuator health.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.health.contributor.HealthIndicator")
    static class Health {

        @Bean
        @ConditionalOnMissingBean
        public PrimosHealthIndicator primosHealthIndicator(PrimosNode node) {
            return new PrimosHealthIndicator(node);
        }
    }
}
