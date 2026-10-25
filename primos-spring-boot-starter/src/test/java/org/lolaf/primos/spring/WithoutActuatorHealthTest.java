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
import org.lolaf.primos.PrimosNode;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs in its own surefire execution, with spring-boot-health left off the classpath: hiding it from the context's
 * class loader is not enough, since the auto-configuration class itself is loaded by the test's.
 */
class WithoutActuatorHealthTest {

    @TempDir
    Path stateDirectory;

    @Test
    void the_node_is_contributed_without_a_health_indicator() {
        assertThatThrownBy(() -> Class.forName("org.springframework.boot.health.contributor.HealthIndicator"))
                .as("spring-boot-health must be off this test's classpath")
                .isInstanceOf(ClassNotFoundException.class);

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PrimosAutoConfiguration.class))
                .withPropertyValues(
                        "primos.application-name=orders",
                        "primos.node-id=1",
                        "primos.bind-port=0",
                        "primos.secret=a-test-secret",
                        "primos.state-directory=" + stateDirectory)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(PrimosNode.class);
                    assertThat(context).doesNotHaveBean("primosHealthIndicator");
                });
    }
}
