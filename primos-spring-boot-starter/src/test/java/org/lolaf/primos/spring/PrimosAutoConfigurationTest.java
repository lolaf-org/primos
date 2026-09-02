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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.Timings;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.transport.Transport;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class PrimosAutoConfigurationTest {

    @TempDir
    Path stateDirectory;

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PrimosAutoConfiguration.class));
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

    @Test
    void contributes_nothing_until_an_application_name_is_set() {
        runner().run(context -> assertThat(context).doesNotHaveBean(PrimosNode.class));
    }

    @Test
    void contributes_the_whole_node_when_an_application_name_is_set() {
        runner().withPropertyValues(singleNodeCluster()).run(context -> {
            assertThat(context).hasSingleBean(PrimosNode.class);
            assertThat(context).hasSingleBean(ClusterConfig.class);
            // Suppliers rather than instances, which is what lets the node rejoin the cluster:
            // each start builds a fresh pair, since a stopped node holds a released lock and a
            // closed socket.
            assertThat(context).hasBean("primosStateStores");
            assertThat(context).hasBean("primosTransports");
            assertThat(context).hasSingleBean(PrimosNodeLifecycle.class);
            assertThat(context.getBean(ClusterConfig.class).applicationName()).isEqualTo("orders");
        });
    }

    @Test
    void the_leader_lifecycle_is_off_unless_it_is_asked_for() {
        runner().withPropertyValues(singleNodeCluster())
                .run(context -> assertThat(context).doesNotHaveBean(LeaderLifecycleController.class));
    }

    @Test
    void vote_logging_is_on_by_default_and_can_be_turned_off() {
        runner().withPropertyValues(singleNodeCluster())
                .run(context -> assertThat(context.getBean(ClusterConfig.class).voteLogging())
                        .isTrue());

        runner().withPropertyValues(singleNodeCluster())
                .withPropertyValues("primos.vote-logging=false")
                .run(context -> assertThat(context.getBean(ClusterConfig.class).voteLogging())
                        .isFalse());
    }

    @Test
    void an_applications_own_timings_win() {
        runner().withPropertyValues(singleNodeCluster())
                .withUserConfiguration(OwnTimings.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(Timings.class);
                    assertThat(context.getBean(Timings.class))
                            .isSameAs(context.getBean(OwnTimings.class).timings);
                });
    }

    @Test
    void a_missing_state_directory_is_refused_with_the_reason() {
        runner().withPropertyValues(
                        "primos.application-name=orders",
                        "primos.node-id=1")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .hasMessageContaining("primos.state-directory is required"));
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnTimings {

        private final Timings timings = Timings.wan();

        @Bean
        Timings primosTimings() {
            return timings;
        }
    }
}
