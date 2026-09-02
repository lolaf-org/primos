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
import org.lolaf.primos.Role;
import org.lolaf.primos.store.FileStateStore;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * {@code primos.lifecycle}: when the node joins, and — in both modes — who makes it leave.
 */
class NodeLifecycleModeTest {

    @TempDir
    Path stateDirectory;

    private ApplicationContextRunner runner(String... extraProperties) {
        String[] base = {
                "primos.application-name=orders",
                "primos.node-id=1",
                "primos.bind-port=0",
                "primos.secret=a-test-secret",
                "primos.state-directory=" + stateDirectory,
        };
        String[] all = new String[base.length + extraProperties.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extraProperties, 0, all, base.length, extraProperties.length);
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PrimosAutoConfiguration.class))
                .withPropertyValues(all);
    }

    @Test
    void context_mode_starts_the_node_with_the_context() {
        runner().run(context ->
                assertThat(context.getBean(PrimosNode.class).isRunning()).isTrue());
    }

    @Test
    void manual_mode_builds_the_node_and_leaves_it_alone() {
        runner("primos.lifecycle=manual").run(context ->
                assertThat(context.getBean(PrimosNode.class).isRunning()).isFalse());
    }

    @Test
    void manual_mode_lets_the_application_start_it_when_it_is_ready() {
        runner("primos.lifecycle=manual").run(context -> {
            PrimosNode node = context.getBean(PrimosNode.class);
            assertThat(node.isRunning()).isFalse();
            node.start();
            assertThat(node.isRunning()).isTrue();
        });
    }

    /**
     * The stop is never manual. This is what sends the handover and the departure announcement, so
     * losing it would cost the cluster an election timeout on every deploy — and would do it
     * silently.
     */
    @Test
    void spring_stops_the_node_on_context_close_in_manual_mode_too() {
        AtomicReference<PrimosNode> node = new AtomicReference<>();
        runner("primos.lifecycle=manual").run(context -> {
            PrimosNode started = context.getBean(PrimosNode.class);
            started.start();
            assertThat(started.isRunning()).isTrue();
            node.set(started);
        });
        // The runner closed the context on the way out of run().
        assertThat(node.get().isRunning()).isFalse();
    }

    @Test
    void spring_stops_the_node_on_context_close_in_context_mode() {
        AtomicReference<PrimosNode> node = new AtomicReference<>();
        runner().run(context -> node.set(context.getBean(PrimosNode.class)));
        assertThat(node.get().isRunning()).isFalse();
    }

    /**
     * The sharp one. {@code stop()} on a node that was never started closes nothing of its own, so
     * a manual-mode application that never got round to starting its node would hold the state
     * directory's exclusive lock — and its socket — until the JVM exited. Opening a second store on
     * the same directory is what proves the lock went.
     *
     * <p>The mechanism changed under this test and the assertion did not, which is the sort of
     * test worth having. Spring used to close the store and transport beans itself, because it
     * made them. Now it contributes <em>suppliers</em> and the node makes each generation's pair,
     * so releasing them is the node's job. Ownership follows creation, and that is the whole rule.
     */
    @Test
    void a_node_that_was_never_started_still_releases_its_state_lock_and_socket() {
        runner("primos.lifecycle=manual").run(context ->
                assertThat(context.getBean(PrimosNode.class).isRunning()).isFalse());

        assertThatCode(() -> {
            try (FileStateStore reopened = new FileStateStore(stateDirectory)) {
                assertThat(reopened.load()).isNotNull();
            }
        }).doesNotThrowAnyException();
    }

    /**
     * The reason any of this exists. A Spring singleton is exactly the shape that cannot be handed
     * back as "build a new one", so an application wanting to leave the cluster for a maintenance
     * window and rejoin had no way to say so — the workaround was a factory bean, which pushes the
     * lifecycle back into the application this module exists to relieve.
     */
    @Test
    void a_node_bean_can_leave_the_cluster_and_rejoin() {
        runner("primos.lifecycle=manual").run(context -> {
            PrimosNode node = context.getBean(PrimosNode.class);

            node.start();
            assertThat(node.isRunning()).isTrue();

            node.stop();
            assertThat(node.isRunning()).isFalse();
            assertThat(node.role())
                    .as("between generations it holds no thread, so it must not claim leadership")
                    .isNotEqualTo(Role.LEADER);

            node.start();
            assertThat(node.isRunning())
                    .as("the same bean, a fresh store and socket underneath it")
                    .isTrue();
        });
    }
}
