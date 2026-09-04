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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.micrometer.PrimosMetrics;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The meters are wired when the application has a registry, and nothing appears when it does not.
 */
class PrimosMetricsAutoConfigurationTest {

    @TempDir
    Path stateDirectory;

    @Test
    void a_registry_is_all_it_takes() {
        runner().withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues(singleNodeCluster())
                .run(context -> {
                    assertThat(context).hasSingleBean(PrimosMetrics.class);
                    MeterRegistry registry = context.getBean(MeterRegistry.class);

                    assertThat(registry.find("primos.leader")
                            .tags("application", "orders", "node", "1").gauge())
                            .as("the gauge half needs the node, so it is bound after the singletons")
                            .isNotNull();
                    assertThat(registry.find("primos.datagrams.dropped").counters())
                            .as("the counters are installed while the node is built, because one"
                                    + " sink fixed at build time cannot be added afterwards")
                            .isNotEmpty();
                });
    }

    @Test
    void nothing_is_contributed_without_a_registry() {
        runner().withPropertyValues(singleNodeCluster())
                .run(context -> assertThat(context).doesNotHaveBean(PrimosMetrics.class));
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(
                PrimosAutoConfiguration.class, PrimosMetricsAutoConfiguration.class));
    }

    private String[] singleNodeCluster() {
        return new String[]{
                "primos.application-name=orders",
                "primos.node-id=1",
                "primos.bind-port=0",
                "primos.secret=a-test-secret",
                "primos.state-directory=" + stateDirectory,
        };
    }
}
