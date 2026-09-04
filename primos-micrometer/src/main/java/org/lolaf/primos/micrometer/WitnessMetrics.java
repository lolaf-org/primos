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
package org.lolaf.primos.micrometer;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.lolaf.primos.PrimosStatsListener;
import org.lolaf.primos.witness.WitnessServer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * A witness server's meters: its routing counters, and the whole of {@link PrimosMetrics} for every
 * cluster it serves.
 *
 * <p>Two halves, for the same reason {@link PrimosMetrics} has two — the per-tenant listeners have
 * to be handed to the builder, and the server's own counters cannot be read until it exists:
 *
 * <pre>
 * WitnessMetrics metrics = WitnessMetrics.create(registry, nodeId);
 *
 * WitnessServer server = WitnessServer.builder()
 *         .configStore(configStore)
 *         .nodeId(nodeId)
 *         .statsListeners(metrics.statsListeners())
 *         .build();
 *
 * metrics.bind(server);
 * </pre>
 *
 * <p>Applications configured in a file are opened by the server's own constructor, so the factory
 * is called during {@code build()} — before {@code bind}. A tenant's meters are therefore
 * registered either when it first appears or when the server arrives, whichever is later, and the
 * factory result is kept so that reconfiguring a tenant does not restart its counters at zero.
 *
 * <p>The routing meters carry {@code node} and no {@code application}, which is the one departure
 * from the mandatory tags: the meaning of {@code unroutable} is precisely that the datagram could
 * not be attributed to a cluster.
 */
public final class WitnessMetrics {

    private static final Map<String, ToDoubleFunction<WitnessServer.RoutingStats>> ROUTING = routingMeters();

    private final MeterRegistry registry;
    private final Tags base;
    private final int nodeId;
    private final Map<String, PrimosMetrics> tenants = new ConcurrentHashMap<>();
    private final Set<String> gauged = ConcurrentHashMap.newKeySet();

    private volatile WitnessServer server;

    public static WitnessMetrics create(MeterRegistry registry, int nodeId) {
        return new WitnessMetrics(registry, nodeId);
    }

    private WitnessMetrics(MeterRegistry registry, int nodeId) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.nodeId = nodeId;
        this.base = Tags.of("node", String.valueOf(nodeId));
    }

    /**
     * The per-application sinks, for {@code WitnessServer.Builder.statsListeners}.
     */
    public Function<String, PrimosStatsListener> statsListeners() {
        return this::listenerFor;
    }

    /**
     * Registers the server's own meters, and any tenant's that was opened before this call.
     */
    public void bind(WitnessServer server) {
        this.server = Objects.requireNonNull(server, "server");

        ROUTING.forEach((reason, value) -> FunctionCounter
                .builder("primos.witness.routing", server, s -> value.applyAsDouble(s.routing()))
                .description("Datagrams that reached no application, and replies that reached no node")
                .tags(base.and("reason", reason))
                .register(registry));

        Gauge.builder("primos.witness.applications", server, s -> s.servedApplications().size())
                .description("Clusters this witness holds a vote for")
                .tags(base.and("state", "served"))
                .strongReference(true)
                .register(registry);
        Gauge.builder("primos.witness.applications", server, s -> s.quarantinedApplications().size())
                .description("Clusters this witness holds a vote for")
                .tags(base.and("state", "quarantined"))
                .strongReference(true)
                .register(registry);

        tenants.keySet().forEach(this::registerTenantGauges);
    }

    private PrimosStatsListener listenerFor(String application) {
        PrimosMetrics metrics = tenants.computeIfAbsent(application,
                name -> PrimosMetrics.forServedApplication(registry, name, nodeId));
        registerTenantGauges(application);
        return metrics;
    }

    /**
     * A served tenant's term and quarantine state, once both the tenant and the server are known.
     */
    private void registerTenantGauges(String application) {
        WitnessServer bound = server;
        if (bound == null || !gauged.add(application)) {
            return;
        }
        Tags tags = base.and("application", application);
        Gauge.builder("primos.witness.term", bound, s -> term(s, application))
                .description("The term this witness holds for one served cluster")
                .tags(tags)
                .strongReference(true)
                .register(registry);
        Gauge.builder("primos.witness.quarantined", bound,
                        s -> s.quarantinedApplications().contains(application) ? 1 : 0)
                .description("1 while a served cluster's node has failed and is being refused")
                .tags(tags)
                .strongReference(true)
                .register(registry);
    }

    /**
     * NaN rather than zero once a cluster is no longer served: a term of 0 is a real value and
     * would read as a witness that had forgotten its vote.
     */
    private static double term(WitnessServer server, String application) {
        return server.servedApplications().contains(application)
                ? server.currentTerm(application)
                : Double.NaN;
    }

    /**
     * Every field of {@code RoutingStats}, as function counters: the server maintains the counts,
     * so this reads them rather than keeping a second set that could drift. They can step
     * backwards when a tenant is removed, since two of them are sums over the served set.
     */
    private static Map<String, ToDoubleFunction<WitnessServer.RoutingStats>> routingMeters() {
        Map<String, ToDoubleFunction<WitnessServer.RoutingStats>> meters = new LinkedHashMap<>();
        meters.put("unroutable", WitnessServer.RoutingStats::unroutable);
        meters.put("unserved_application", WitnessServer.RoutingStats::unservedApplication);
        meters.put("auto_registrations_refused", WitnessServer.RoutingStats::autoRegistrationsRefused);
        meters.put("unusable_name", WitnessServer.RoutingStats::unusableName);
        meters.put("failed_application", WitnessServer.RoutingStats::droppedForFailedApplication);
        meters.put("auto_registrations", WitnessServer.RoutingStats::autoRegistrations);
        meters.put("awaiting_node_config", WitnessServer.RoutingStats::awaitingNodeConfig);
        meters.put("registration_bad_tag", WitnessServer.RoutingStats::registrationBadTag);
        meters.put("inbox_overflowed", WitnessServer.RoutingStats::inboxOverflowed);
        meters.put("undeliverable_replies", WitnessServer.RoutingStats::undeliverableReplies);
        return meters;
    }
}
