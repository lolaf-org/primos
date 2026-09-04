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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.store.FileStateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.UdpTransport;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.ConfigStore;
import org.lolaf.primos.witness.WitnessServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The README's examples, compiled and run.
 *
 * <p>Same rule as the library's own: an API change that makes the README wrong breaks the build
 * rather than quietly misleading the next reader.
 */
class ReadmeExamplesTest {

    @TempDir
    Path directory;

    @Test
    void the_node_example_binds_and_reports() {
        MeterRegistry registry = new SimpleMeterRegistry();

        ClusterConfig cluster = ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(1)
                .peer(Peer.application(2, "127.0.0.1", 7421))
                .build();

        PrimosMetrics metrics = PrimosMetrics.create(registry, cluster);

        PrimosNode node = PrimosNode.builder()
                .clusterConfig(cluster)
                .statsListener(metrics)
                .store(() -> new FileStateStore(directory))
                .transport(UdpTransport.builder()
                        .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                        .peers(cluster.allPeers()))
                .build();

        node.start();
        metrics.bind(node);

        try {
            assertThat(registry.get("primos.leader")
                    .tags("application", "orders", "node", "1").gauge().value()).isZero();
        } finally {
            node.stop();
        }
    }

    @Test
    void the_witness_example_binds() throws IOException {
        MeterRegistry registry = new SimpleMeterRegistry();
        int nodeId = Peer.MAX_ID;
        ConfigStore configStore = witnessConfiguration();

        WitnessMetrics metrics = WitnessMetrics.create(registry, nodeId);

        WitnessServer server = WitnessServer.builder()
                .configStore(configStore)
                .nodeId(nodeId)
                .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                .statsListeners(metrics.statsListeners())
                .build();

        metrics.bind(server);

        try {
            assertThat(registry.get("primos.witness.applications")
                    .tags("node", String.valueOf(nodeId), "state", "served").gauge().value())
                    .isZero();
        } finally {
            server.close();
        }
    }

    private ConfigStore witnessConfiguration() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        Files.write(directory.resolve("witness.properties"),
                ("secret.shared.value=a-shared-cluster-secret\n"
                        + "state.directory=" + directory.resolve("state") + "\n")
                        .getBytes(StandardCharsets.UTF_8));
        return new FileConfigStore(directory, MonotonicClock.SYSTEM);
    }
}
