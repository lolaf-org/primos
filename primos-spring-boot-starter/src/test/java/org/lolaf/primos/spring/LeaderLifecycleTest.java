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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * The reason the transitions have a thread of their own.
     *
     * <p>Stopping a message listener container drains it and can take tens of seconds. Run on the
     * thread that reported the role change, that stalls whatever is on the other end of it — the
     * node thread, when no listener executor is configured — which misses heartbeats, triggers a
     * spurious election and loses the leadership the stop was reacting to, which triggers another
     * stop. The executor is what makes that impossible, and until now nothing said so.
     *
     * <p>The stop blocks on a latch rather than sleeping, so this asserts the design rather than
     * the speed of the machine: while the latch is held the stop cannot have finished, so a
     * controller that ran it inline could not have returned. The wait is bounded so that such a
     * controller fails this test rather than hanging it.
     */
    @Test
    void a_slow_stop_does_not_hold_the_thread_that_reported_the_role_change() {
        runner().withUserConfiguration(SlowBean.class).run(context -> {
            SlowStoppingBean gated = context.getBean(SlowStoppingBean.class);
            LeaderLifecycleController controller = context.getBean(LeaderLifecycleController.class);

            controller.onRoleChanged(Role.CANDIDATE, Role.LEADER, 4);
            eventually(() -> assertThat(gated.isRunning()).isTrue());

            gated.blockStops();
            try {
                long before = System.nanoTime();
                controller.onRoleChanged(Role.LEADER, Role.FOLLOWER, 5);
                Duration held = Duration.ofNanos(System.nanoTime() - before);

                assertThat(held)
                        .as("reporting a role change must not wait for the gated beans to stop")
                        .isLessThan(Duration.ofSeconds(1));
                // The negative twin. Without it a controller that stopped nothing whatever would
                // pass the assertion above, which is the cheapest way to be fast.
                assertThat(gated.isRunning())
                        .as("the stop is genuinely in flight and genuinely slow")
                        .isTrue();
            } finally {
                gated.releaseStops();
            }

            eventually(() -> assertThat(gated.isRunning()).isFalse());
        });
    }

    /**
     * The other half of what a single thread buys: a transition submitted while a slow one is in
     * flight is queued behind it rather than dropped or interleaved. Leadership regained during a
     * stop is the realistic way to get here, and a start that went missing would leave a leader
     * with everything switched off — silent and indefinite.
     */
    @Test
    void a_transition_arriving_during_a_slow_stop_is_applied_after_it() {
        runner().withUserConfiguration(SlowBean.class).run(context -> {
            SlowStoppingBean gated = context.getBean(SlowStoppingBean.class);
            LeaderLifecycleController controller = context.getBean(LeaderLifecycleController.class);

            controller.onRoleChanged(Role.CANDIDATE, Role.LEADER, 4);
            eventually(() -> assertThat(gated.isRunning()).isTrue());

            gated.blockStops();
            try {
                controller.onRoleChanged(Role.LEADER, Role.FOLLOWER, 5);
                // Leadership back before the stop has finished. Both calls return immediately; the
                // executor holds the second until the first is done.
                controller.onRoleChanged(Role.FOLLOWER, Role.LEADER, 6);
            } finally {
                gated.releaseStops();
            }

            eventually(() -> {
                assertThat(gated.isRunning())
                        .as("the start for term 6 must survive the stop it was queued behind")
                        .isTrue();
                assertThat(gated.stops()).isEqualTo(1);
                assertThat(gated.starts()).isEqualTo(2);
            });
        });
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

    /**
     * A gated bean whose stop blocks until the test lets go — a listener container draining, in
     * miniature.
     *
     * <p>Not auto-start, unlike its parent. An auto-starting bean is stopped once during refresh,
     * and a slow stop there would hold up the context rather than a transition, which is a
     * different thing from the one these two tests are about.
     */
    @LeaderOnly
    static class SlowStoppingBean extends AutoStartingBean {

        private final AtomicReference<CountDownLatch> gate =
                new AtomicReference<>(new CountDownLatch(0));

        void blockStops() {
            gate.set(new CountDownLatch(1));
        }

        void releaseStops() {
            gate.get().countDown();
        }

        @Override
        public void stop() {
            try {
                // Bounded, so a controller that stops inline fails these tests rather than hanging
                // them. A test that can hang reports nothing at all.
                gate.get().await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.stop();
        }

        @Override
        public boolean isAutoStartup() {
            return false;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SlowBean {

        @Bean
        SlowStoppingBean slow() {
            return new SlowStoppingBean();
        }
    }
}
