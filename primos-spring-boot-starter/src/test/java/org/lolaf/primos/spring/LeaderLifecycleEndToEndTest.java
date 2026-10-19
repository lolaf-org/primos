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
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The whole chain on a node that really wins: election, listener dispatch off the node thread, the
 * transition executor, and the gated beans.
 *
 * <p>A single-node cluster, so quorum is one and the node elects itself within an election timeout.
 * That is a legitimate configuration rather than a test fixture — it is what lets an application run
 * the same code path in a non-clustered deployment.
 */
class LeaderLifecycleEndToEndTest {

    @TempDir
    Path stateDirectory;

    private static void eventually(Runnable assertion) {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(assertion::run);
    }

    private ApplicationContextRunner runner(String... extraProperties) {
        String[] base = {
                "primos.application-name=orders",
                "primos.node-id=1",
                "primos.bind-port=0",
                "primos.secret=a-test-secret",
                "primos.state-directory=" + stateDirectory,
                "primos.leader-lifecycle.enabled=true",
        };
        String[] all = new String[base.length + extraProperties.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extraProperties, 0, all, base.length, extraProperties.length);
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PrimosAutoConfiguration.class))
                .withPropertyValues(all)
                .withUserConfiguration(GatedAndListening.class);
    }

    @Test
    void winning_an_election_really_does_start_the_gated_beans() {
        runner().run(context -> {
            AutoStartingBean gated = context.getBean(AutoStartingBean.class);
            assertThat(gated.isRunning())
                    .as("not before the election has been won")
                    .isFalse();

            eventually(() -> {
                assertThat(context.getBean(PrimosNode.class).role()).isEqualTo(Role.LEADER);
                assertThat(gated.isRunning()).isTrue();
            });
        });
    }

    @Test
    void leadership_reaches_an_ordinary_event_listener() {
        runner().run(context -> {
            RoleRecorder recorder = context.getBean(RoleRecorder.class);

            eventually(() -> assertThat(recorder.roles()).contains(Role.LEADER));
        });
    }

    @Test
    void a_published_change_reaches_an_ordinary_event_listener() {
        runner().run(context -> {
            PrimosNode node = context.getBean(PrimosNode.class);
            eventually(() -> assertThat(node.role()).isEqualTo(Role.LEADER));

            long term = node.publishChange().orElseThrow(AssertionError::new).term();

            eventually(() -> assertThat(context.getBean(RoleRecorder.class).versions())
                    .containsExactly(term + "/1/self"));
        });
    }

    /**
     * The pair that matters for {@code manual}: nothing runs until the application says so, and
     * then everything does. The negative half is what proves the positive one is not passing by
     * accident.
     */
    @Test
    void in_manual_mode_nothing_runs_until_the_application_starts_the_node() {
        runner("primos.lifecycle=manual").run(context -> {
            AutoStartingBean gated = context.getBean(AutoStartingBean.class);
            PrimosNode node = context.getBean(PrimosNode.class);
            PrimosNodeLifecycle lifecycle = context.getBean(PrimosNodeLifecycle.class);

            assertThat(lifecycle.isWaitingOnAManualStart())
                    .as("configured, gated and not started is the one silent failure this mode adds")
                    .isTrue();

            // Long enough for an election timeout to have passed several times over.
            Thread.sleep(1_000);
            assertThat(gated.isRunning())
                    .as("a node that was never started cannot have won anything")
                    .isFalse();
            assertThat(gated.starts()).isEqualTo(1);

            node.start();

            assertThat(lifecycle.isWaitingOnAManualStart()).isFalse();
            eventually(() -> assertThat(gated.isRunning()).isTrue());
        });
    }

    /**
     * The reason {@code prepare()} runs on the transition thread rather than the caller's. Under
     * {@code manual} an application may start its node from any thread it likes, and then nothing
     * orders refresh's writes against the transition thread's reads unless the controller arranges
     * it. A thread that is not the one that ran refresh is the shape of that.
     */
    @Test
    void manual_start_from_another_thread_still_gates_correctly() {
        runner("primos.lifecycle=manual").run(context -> {
            AutoStartingBean gated = context.getBean(AutoStartingBean.class);
            PrimosNode node = context.getBean(PrimosNode.class);
            assertThat(gated.isRunning()).isFalse();

            Thread starter = new Thread(node::start, "an-application-thread");
            starter.start();
            starter.join();

            eventually(() -> assertThat(gated.isRunning()).isTrue());
        });
    }

    @LeaderOnly
    static class GatedBean extends AutoStartingBean {
    }

    static class RoleRecorder {

        private final List<Role> roles = new CopyOnWriteArrayList<>();

        private final List<String> versions = new CopyOnWriteArrayList<>();

        @EventListener
        void onRoleChanged(PrimosEvents.RoleChanged event) {
            roles.add(event.current());
        }

        @EventListener
        void onLeaderVersion(PrimosEvents.LeaderVersion event) {
            versions.add(event.term() + "/" + event.version() + (event.leader().isSelf() ? "/self" : ""));
        }

        List<Role> roles() {
            return roles;
        }

        List<String> versions() {
            return versions;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class GatedAndListening {

        @Bean
        GatedBean gated() {
            return new GatedBean();
        }

        @Bean
        RoleRecorder roleRecorder() {
            return new RoleRecorder();
        }
    }
}
