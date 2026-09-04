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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.Peer;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.WitnessServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness's own counters, and one set of node meters per cluster it serves.
 */
class WitnessMetricsTest {

    private static final int WITNESS_ID = Peer.MAX_ID;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @TempDir
    Path directory;
    private WitnessServer server;

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        write(directory.resolve("witness.properties"),
                "secret.shared.value=a-shared-cluster-secret\n"
                        + "state.directory=" + directory.resolve("state") + "\n");
    }

    @AfterEach
    void closeServer() {
        if (server != null) {
            server.close();
        }
    }

    /**
     * These are the witness's first counters to reach anything but a {@code toString}, and the one
     * to alert on is {@code undeliverable_replies}: a witness that silently fails to answer looks
     * exactly like a witness nobody is asking, while the cluster on the other end waits out an
     * election timeout for a vote that was never coming.
     */
    @Test
    void the_routing_counters_reach_the_registry_and_move() throws IOException {
        WitnessMetrics metrics = start();

        assertThat(registry.find("primos.witness.routing").functionCounters())
                .as("every field of RoutingStats, one series each")
                .hasSize(10);
        assertThat(routing("undeliverable_replies")).isZero();

        sendJunk();

        assertThat(routing("unroutable"))
                .as("three bytes cannot carry an application name, so it belongs to no tenant")
                .isEqualTo(1.0);
        assertThat(metrics).isNotNull();
    }

    /**
     * The tenants here are configured in a file, so the server's own constructor opens them and the
     * stats-listener factory runs during {@code build()} — before {@code bind}. Their gauges have
     * to arrive anyway, which is why binding registers what it finds already open.
     */
    @Test
    void a_tenant_opened_before_binding_still_gets_its_meters() {
        application("orders");
        application("billing");
        start();

        assertThat(gauge("primos.witness.term", "application", "orders")).isZero();
        assertThat(gauge("primos.witness.quarantined", "application", "billing")).isZero();
        assertThat(gauge("primos.witness.applications", "state", "served")).isEqualTo(2.0);
        assertThat(gauge("primos.witness.applications", "state", "quarantined")).isZero();
    }

    /**
     * One tenant's counters added to another's is a number nobody can act on, so the node meters
     * are tagged by the tenant and the witness's own id.
     */
    @Test
    void every_served_cluster_gets_its_own_node_meters() {
        application("orders");
        application("billing");
        start();

        assertThat(registry.find("primos.datagrams.dropped")
                .tags("application", "orders", "node", String.valueOf(WITNESS_ID))
                .counters())
                .as("a full set of drop reasons for this tenant alone")
                .hasSize(7);
        assertThat(registry.find("primos.probe.rtt").tag("application", "billing").timers())
                .as("a witness learns its voters, so its peer tag collapses to one series")
                .hasSize(1);
        assertThat(registry.find("primos.probe.rtt").tag("application", "billing").timer()
                .getId().getTag("peer")).isEqualTo("learned");
    }

    // ------------------------------------------------------------- helpers

    private WitnessMetrics start() {
        WitnessMetrics metrics = WitnessMetrics.create(registry, WITNESS_ID);
        server = WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                .statsListeners(metrics.statsListeners())
                .build();
        metrics.bind(server);
        return metrics;
    }

    private void sendJunk() throws IOException {
        try (DatagramChannel client = DatagramChannel.open()) {
            client.send(ByteBuffer.wrap(new byte[]{1, 2, 3}), server.boundAddress());
        }
        server.runOnce(TimeUnit.MILLISECONDS.toNanos(200));
    }

    private double routing(String reason) {
        var found = registry.find("primos.witness.routing").tag("reason", reason).functionCounter();
        return found == null ? Double.NaN : found.count();
    }

    private double gauge(String name, String... tags) {
        var found = registry.find(name).tags(tags).gauge();
        return found == null ? Double.NaN : found.value();
    }

    private void application(String name) {
        write(directory.resolve("applications/" + name + ".properties"), "");
    }

    private void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
