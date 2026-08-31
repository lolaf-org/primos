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
import org.lolaf.primos.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The health indicator, whose one expensive mistake would be reporting a healthy follower as down.
 */
class PrimosHealthIndicatorTest {

    @TempDir
    Path stateDirectory;

    @Test
    void a_healthy_follower_is_up() {
        Health health = new PrimosHealthIndicator(new StubNode(Role.FOLLOWER, 7, true)).health();

        assertThat(health.getStatus())
                .as("a follower is doing exactly what it should; reporting it DOWN takes every"
                        + " follower out of a load balancer during an ordinary election")
                .isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("role", "FOLLOWER")
                .containsEntry("term", 7L)
                .containsEntry("state", "running");
    }

    @Test
    void a_leader_is_up() {
        Health health = new PrimosHealthIndicator(new StubNode(Role.LEADER, 7, true)).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("role", "LEADER");
    }

    @Test
    void only_a_failed_node_is_down() {
        Health health = new PrimosHealthIndicator(new StubNode(Role.FAILED, 7, true)).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsKey("reason");
    }

    /**
     * Not started is not a failure — under {@code primos.lifecycle=manual} it is the expected state
     * until the application is ready — but it must not read as an ordinary follower, because a
     * deployment that forgot to start its node looks identical to a healthy one from every other
     * angle.
     */
    @Test
    void a_node_that_was_never_started_says_so_and_is_still_up() {
        Health health = new PrimosHealthIndicator(new StubNode(Role.FOLLOWER, 0, false)).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("state", "not-started");
    }

    @Test
    void it_is_contributed_by_the_auto_configuration() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PrimosAutoConfiguration.class))
                .withPropertyValues(
                        "primos.application-name=orders",
                        "primos.node-id=1",
                        "primos.bind-port=0",
                        "primos.secret=a-test-secret",
                        "primos.state-directory=" + stateDirectory)
                .run(context -> assertThat(context).hasSingleBean(PrimosHealthIndicator.class));
    }

    /**
     * A stub rather than a real node, because {@link Role#FAILED} needs a node whose thread has
     * died and there is no honest way to arrange that from here. The indicator is a pure function
     * of what it is handed, so a stub tests all of it.
     */
    private record StubNode(Role role, long term, boolean running) implements PrimosNode {

        @Override
        public void start() {
            // nothing to do
        }

        @Override
        public void stop() {
            // nothing to do
        }

        @Override
        public RoleAndTerm roleAndTerm() {
            return new RoleAndTerm(role, term);
        }

        @Override
        public Optional<Leader> leader() {
            return Optional.empty();
        }

        @Override
        public void addListener(PrimosListener listener) {
            // nothing to do
        }

        @Override
        public PrimosStats stats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public ClusterConfig config() {
            return ClusterConfig.builder()
                    .applicationName("orders")
                    .nodeId(1)
                    .peer(Peer.application(2, "127.0.0.1", 1))
                    .build();
        }
    }
}
