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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * An {@code @EventListener} runs off the node thread, in order, and however slow it is the node keeps working.
 */
class ListenerDispatchTest {

    @TempDir
    Path stateDirectory;

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PrimosAutoConfiguration.class))
                .withUserConfiguration(BlockedListenerConfiguration.class)
                .withPropertyValues(
                        "primos.application-name=orders",
                        "primos.node-id=1",
                        "primos.bind-port=0",
                        "primos.secret=a-test-secret",
                        "primos.state-directory=" + stateDirectory);
    }

    @Test
    void a_blocked_listener_does_not_stall_the_node() {
        runner().run(context -> {
            BlockedListener listener = context.getBean(BlockedListener.class);
            PrimosNode node = context.getBean(PrimosNode.class);
            try {
                await().atMost(10, TimeUnit.SECONDS).until(() -> node.leadershipConfirmedNanos().isPresent());
                long first = node.leadershipConfirmedNanos().getAsLong();

                await().atMost(10, TimeUnit.SECONDS)
                        .alias("the loop is still turning while the listener holds the only worker")
                        .until(() -> node.leadershipConfirmedNanos().getAsLong() > first);
                assertThat(listener.roles).as("and the listener really is still blocked").isEmpty();
            } finally {
                listener.release.countDown();
            }
        });
    }

    @Test
    void events_held_up_by_a_blocked_listener_all_arrive_in_order() {
        runner().run(context -> {
            BlockedListener listener = context.getBean(BlockedListener.class);
            PrimosNode node = context.getBean(PrimosNode.class);
            await().atMost(10, TimeUnit.SECONDS).until(() -> node.role().isLeader());
            int publications = 6;
            for (int i = 0; i < publications; i++) {
                node.publishChange();
                // Announced once a heartbeat, so publishing faster than that coalesces them.
                Thread.sleep(150);
            }

            listener.release.countDown();

            await().atMost(10, TimeUnit.SECONDS).until(() -> listener.versions.size() == publications);
            List<Long> ordered = new ArrayList<>(listener.versions);
            ordered.sort(null);
            assertThat(listener.versions).as("one worker, first in first out").isEqualTo(ordered).doesNotHaveDuplicates();
            assertThat(listener.roles).containsExactly("FOLLOWER->CANDIDATE", "CANDIDATE->LEADER");
        });
    }

    @Test
    void listeners_run_on_the_events_thread_not_the_node_thread() {
        runner().run(context -> {
            BlockedListener listener = context.getBean(BlockedListener.class);
            listener.release.countDown();

            await().atMost(10, TimeUnit.SECONDS).until(() -> !listener.threads.isEmpty());

            assertThat(listener.threads).allSatisfy(name -> assertThat(name).isEqualTo("primos-orders-1-events"));
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class BlockedListenerConfiguration {

        @Bean
        BlockedListener blockedListener() {
            return new BlockedListener();
        }
    }

    static class BlockedListener {

        final CountDownLatch release = new CountDownLatch(1);
        final List<String> roles = new CopyOnWriteArrayList<>();
        final List<Long> versions = new CopyOnWriteArrayList<>();
        final List<String> threads = new CopyOnWriteArrayList<>();

        @EventListener
        void onRoleChanged(PrimosEvents.RoleChanged event) throws InterruptedException {
            threads.add(Thread.currentThread().getName());
            release.await(30, TimeUnit.SECONDS);
            roles.add(event.previous() + "->" + event.current());
        }

        @EventListener
        void onLeaderVersion(PrimosEvents.LeaderVersion event) {
            versions.add(event.version());
        }
    }
}
