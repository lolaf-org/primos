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
package org.lolaf.primos.witness.boot;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.micrometer.WitnessMetrics;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness's counters reach a dashboard for the first time here, and the per-tenant half is the
 * part with an ordering to get wrong.
 */
class WitnessMetricsAutoConfigurationTest {

    @TempDir
    Path configDirectory;

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(configDirectory.resolve("applications"));
        write(configDirectory.resolve("witness.properties"), "bind.host=127.0.0.1\nbind.port=0\n"
                + "secret.shared.value=a-shared-cluster-secret\n"
                + "state.directory=" + configDirectory.resolve("state") + "\n");
        write(configDirectory.resolve("applications/orders.properties"), "");
    }

    @Test
    void a_registry_binds_the_witness_and_every_cluster_it_serves() {
        runner().withBean(MeterRegistry.class, SimpleMeterRegistry::new).run(context -> {
            assertThat(context).hasSingleBean(WitnessMetrics.class);
            MeterRegistry registry = context.getBean(MeterRegistry.class);

            assertThat(registry.find("primos.witness.routing").functionCounters()).hasSize(10);
            assertThat(registry.find("primos.witness.term").tag("application", "orders").gauge())
                    .as("a tenant named in the configuration directory is opened by the server's"
                            + " own constructor, so its gauges are registered when bind catches up")
                    .isNotNull();
            assertThat(registry.find("primos.datagrams.dropped").tag("application", "orders").counters())
                    .as("its sink had to be handed to the builder, since the constructor is what"
                            + " opens it")
                    .isNotEmpty();
        });
    }

    @Test
    void the_server_is_built_and_serving_without_a_registry() {
        runner().run(context -> {
            assertThat(context).doesNotHaveBean(WitnessMetrics.class);
            assertThat(context.getBean(WitnessServer.class).servedApplications()).contains("orders");
        });
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(WitnessAutoConfiguration.class,
                        WitnessMetricsAutoConfiguration.class))
                .withPropertyValues("primos.witness.config-directory=" + configDirectory);
    }

    private void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
