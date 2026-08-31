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
import org.lolaf.primos.Role;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The leadership-gated lifecycle.
 *
 * <p>The node in these tests is configured with a peer that does not exist, so quorum is two and it
 * can never elect itself. Leadership is then delivered to the controller by hand, which makes every
 * assertion here about the controller's own logic rather than about election timing.
 * {@link LeaderLifecycleEndToEndTest} covers a node that really does win.
 */
class LeaderLifecycleTest {

    @TempDir
    Path stateDirectory;

    private ApplicationContextRunner runner(String... extraProperties) {
        String[] base = {
                "primos.application-name=orders",
                "primos.node-id=1",
                "primos.bind-port=0",
                "primos.secret=a-test-secret",
                "primos.state-directory=" + stateDirectory,
                // A peer that will never answer, so quorum is 2 and this node never leads by itself.
                "primos.peers[0].id=2",
                "primos.peers[0].host=127.0.0.1",
                "primos.peers[0].port=1",
                "primos.leader-lifecycle.enabled=true",
        };
        String[] all = new String[base.length + extraProperties.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extraProperties, 0, all, base.length, extraProperties.length);
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PrimosAutoConfiguration.class))
                .withPropertyValues(all);
    }

    private static void eventually(Runnable assertion) {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(assertion::run);
    }

    /**
     * The trap this whole class exists for: a {@code SmartLifecycle} that auto-starts is running
     * before the first election has concluded, so a follower would be consuming from its queues
     * within a second of boot with nothing anywhere to say so.
     */
    @Test
    void a_gated_bean_that_auto_starts_is_stopped_before_the_node_can_lead() {
        runner().withUserConfiguration(AnnotatedBean.class).run(context -> {
            AutoStartingBean gated = context.getBean(AutoStartingBean.class);
            assertThat(gated.isRunning())
                    .as("a gated bean must not be running when the context has finished refreshing")
                    .isFalse();
            assertThat(gated.starts()).isEqualTo(1);
            assertThat(gated.stops()).isEqualTo(1);
        });
    }

    @Test
    void gaining_leadership_starts_the_gated_beans() {
        runner().withUserConfiguration(AnnotatedBean.class).run(context -> {
            AutoStartingBean gated = context.getBean(AutoStartingBean.class);
            LeaderLifecycleController controller = context.getBean(LeaderLifecycleController.class);

            controller.onRoleChanged(Role.CANDIDATE, Role.LEADER, 4);

            eventually(() -> assertThat(gated.isRunning()).isTrue());
        });
    }

    @Test
    void losing_leadership_stops_them_again() {
        runner().withUserConfiguration(AnnotatedBean.class).run(context -> {
            AutoStartingBean gated = context.getBean(AutoStartingBean.class);
            LeaderLifecycleController controller = context.getBean(LeaderLifecycleController.class);

            controller.onRoleChanged(Role.CANDIDATE, Role.LEADER, 4);
            eventually(() -> assertThat(gated.isRunning()).isTrue());

            controller.onRoleChanged(Role.LEADER, Role.FOLLOWER, 5);
            eventually(() -> assertThat(gated.isRunning()).isFalse());
        });
    }

    /**
     * Leadership can be lost and regained faster than a stop completes. Without the term check a
     * stop from term 5 landing after the start for term 6 leaves a leader with everything switched
     * off — silent, indefinite, and the worst failure this class can have.
     */
    @Test
    void a_stale_transition_is_dropped_by_term() {
        runner().withUserConfiguration(AnnotatedBean.class).run(context -> {
            AutoStartingBean gated = context.getBean(AutoStartingBean.class);
            LeaderLifecycleController controller = context.getBean(LeaderLifecycleController.class);

            controller.onRoleChanged(Role.CANDIDATE, Role.LEADER, 6);
            eventually(() -> assertThat(gated.isRunning()).isTrue());

            // A stop from an earlier term, arriving late.
            controller.onRoleChanged(Role.LEADER, Role.FOLLOWER, 5);

            eventually(() -> assertThat(gated.isRunning())
                    .as("a stop from term 5 must not switch off a leader of term 6")
                    .isTrue());
        });
    }

    @Test
    void an_explicit_bean_list_wins_over_the_annotation() {
        runner("primos.leader-lifecycle.beans=listed")
                .withUserConfiguration(AnnotatedAndListedBeans.class)
                .run(context -> {
                    AutoStartingBean listed = context.getBean("listed", AutoStartingBean.class);
                    AutoStartingBean annotated = context.getBean("annotated", AutoStartingBean.class);
                    LeaderLifecycleController controller =
                            context.getBean(LeaderLifecycleController.class);

                    assertThat(listed.isRunning()).isFalse();
                    assertThat(annotated.isRunning())
                            .as("the annotation is ignored once beans are named explicitly")
                            .isTrue();

                    controller.onRoleChanged(Role.CANDIDATE, Role.LEADER, 2);
                    eventually(() -> assertThat(listed.isRunning()).isTrue());
                });
    }

    @Test
    void context_scope_gates_every_lifecycle_bean_but_not_primos_own() {
        runner("primos.leader-lifecycle.scope=context")
                .withUserConfiguration(PlainLifecycleBean.class)
                .run(context -> {
                    AutoStartingBean plain = context.getBean(AutoStartingBean.class);
                    assertThat(plain.isRunning())
                            .as("an unannotated Lifecycle bean is gated under scope=context")
                            .isFalse();

                    assertThat(context.getBean(PrimosNodeLifecycle.class).isRunning())
                            .as("gating primos' own lifecycle would stop the thing that reports"
                                    + " leadership")
                            .isTrue();

                    context.getBean(LeaderLifecycleController.class)
                            .onRoleChanged(Role.CANDIDATE, Role.LEADER, 1);
                    eventually(() -> assertThat(plain.isRunning()).isTrue());
                });
    }

    /**
     * {@code prepare()} now runs on the transition thread and blocks, so its exceptions travel
     * through an {@link java.util.concurrent.ExecutionException}. A bean name that does not exist
     * is the realistic way to reach that path, and it must still fail the context wearing its own
     * message rather than one wrapped in a threading detail.
     */
    @Test
    void naming_a_bean_that_does_not_exist_fails_the_context_with_its_own_message() {
        runner("primos.leader-lifecycle.beans=nosuchbean")
                .withUserConfiguration(AnnotatedBean.class)
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .hasStackTraceContaining("nosuchbean"));
    }

    @LeaderOnly
    static class GatedBean extends AutoStartingBean {
    }

    @Configuration(proxyBeanMethods = false)
    static class AnnotatedBean {

        @Bean
        GatedBean annotated() {
            return new GatedBean();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AnnotatedAndListedBeans {

        @Bean
        GatedBean annotated() {
            return new GatedBean();
        }

        @Bean
        AutoStartingBean listed() {
            return new AutoStartingBean();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PlainLifecycleBean {

        @Bean
        AutoStartingBean plain() {
            return new AutoStartingBean();
        }
    }
}
