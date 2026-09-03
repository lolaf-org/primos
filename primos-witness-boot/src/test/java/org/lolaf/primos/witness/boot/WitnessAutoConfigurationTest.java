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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.Peer;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.witness.ConfigStore;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class WitnessAutoConfigurationTest {

    @TempDir
    Path configDirectory;

    @Test
    void builds_a_server_from_the_configuration_directory() {
        runner().withPropertyValues(configured("primos.witness.bind.host=127.0.0.1",
                "primos.witness.bind.port=0")).run(context -> {
            assertThat(context).hasSingleBean(WitnessServer.class);
            assertThat(context).hasSingleBean(ConfigStore.class);
            assertThat(context.getBean(WitnessServer.class).boundAddress().getAddress().getHostAddress())
                    .isEqualTo("127.0.0.1");
        });
    }

    @Test
    void the_three_settings_are_the_tarball_s_three_options() {
        runner().withPropertyValues(configured("primos.witness.node-id=7",
                "primos.witness.bind.host=127.0.0.1", "primos.witness.bind.port=0")).run(context -> {
            WitnessProperties properties = context.getBean(WitnessProperties.class);
            assertThat(properties.configDirectory()).isEqualTo(configDirectory);
            assertThat(properties.nodeId()).isEqualTo(7);
            assertThat(properties.bind().host()).isEqualTo("127.0.0.1");
        });
    }

    @Test
    void the_node_id_defaults_to_the_top_of_the_range_as_the_tarball_does() {
        runner().withPropertyValues(configured("primos.witness.bind.host=127.0.0.1",
                "primos.witness.bind.port=0")).run(context ->
                assertThat(context.getBean(WitnessProperties.class).nodeId()).isEqualTo(Peer.MAX_ID));
    }

    /**
     * The common container case: the platform decides the port and nobody wants to restate the
     * host to say so.
     */
    @Test
    void the_port_may_be_given_without_restating_the_host() {
        settings("bind.host=127.0.0.1\nbind.port=17420\n");
        runner().withPropertyValues(configured("primos.witness.bind.port=0")).run(context -> {
            java.net.InetSocketAddress bound = context.getBean(WitnessServer.class).boundAddress();
            assertThat(bound.getAddress().getHostAddress())
                    .as("the half that was not given comes from the file, not from a default"
                            + " invented in the Spring module")
                    .isEqualTo("127.0.0.1");
            assertThat(bound.getPort())
                    .as("and the half that was given wins")
                    .isNotEqualTo(17420);
        });
    }

    @Test
    void the_file_decides_where_to_bind_when_spring_says_nothing() {
        settings("bind.host=127.0.0.1\nbind.port=0\n");
        runner().withPropertyValues("primos.witness.config-directory=" + configDirectory)
                .run(context -> assertThat(context.getBean(WitnessServer.class).boundAddress()
                        .getAddress().getHostAddress()).isEqualTo("127.0.0.1"));
    }

    /**
     * The rule stated as a test: what Spring does not bind is not merely absent from
     * {@link WitnessProperties}, it is still live in the file — which is what keeps it re-readable.
     */
    @Test
    void everything_spring_does_not_bind_is_still_read_from_the_file() {
        settings("bind.host=127.0.0.1\nbind.port=0\n"
                + "state.directory=" + configDirectory.resolve("state") + "\n"
                + "config.source=cluster\n"
                + "secret.shared.value=a-shared-cluster-secret\n");
        runner().withPropertyValues("primos.witness.config-directory=" + configDirectory)
                .run(context -> {
                    var settings = context.getBean(ConfigStore.class).current().settings();
                    assertThat(settings.stateDirectory()).isEqualTo(configDirectory.resolve("state"));
                    assertThat(settings.configSource().isCluster()).isTrue();
                    assertThat(settings.keyring().isEmpty()).isFalse();
                });
    }

    @Test
    void a_missing_configuration_directory_names_the_property_to_fix() {
        runner().withPropertyValues("primos.witness.config-directory=" + configDirectory.resolve("absent"))
                .run(context -> assertThat(context).getFailure()
                        .hasMessageContaining("primos.witness.config-directory"));
    }

    @Test
    void contributes_the_health_indicator_and_the_lifecycle() {
        runner().withPropertyValues(configured("primos.witness.bind.host=127.0.0.1",
                "primos.witness.bind.port=0")).run(context -> {
            assertThat(context).hasSingleBean(WitnessServerLifecycle.class);
            assertThat(context).hasSingleBean(WitnessHealthIndicator.class);
        });
    }

    /**
     * How the opt-in properties-only store arrives without a switch in the auto-configuration.
     */
    @Test
    void an_application_may_supply_its_own_config_store() {
        runner().withUserConfiguration(OwnConfigStore.class)
                .withPropertyValues("primos.witness.config-directory=" + configDirectory)
                .run(context -> {
                    assertThat(context).hasSingleBean(ConfigStore.class);
                    assertThat(context).hasBean("theApplicationsOwnStore");
                });
    }

    // ---------------------------------------------------------------- plumbing

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(configDirectory.resolve("applications"));
        settings("bind.host=127.0.0.1\nbind.port=0\n");
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(WitnessAutoConfiguration.class));
    }

    private String[] configured(String... properties) {
        String[] all = new String[properties.length + 1];
        all[0] = "primos.witness.config-directory=" + configDirectory;
        System.arraycopy(properties, 0, all, 1, properties.length);
        return all;
    }

    private void settings(String contents) {
        try {
            Files.write(configDirectory.resolve("witness.properties"),
                    contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnConfigStore {

        @Bean(destroyMethod = "close")
        ConfigStore theApplicationsOwnStore(WitnessProperties properties) {
            return new FileConfigStore(properties.configDirectory(), MonotonicClock.SYSTEM);
        }
    }
}
