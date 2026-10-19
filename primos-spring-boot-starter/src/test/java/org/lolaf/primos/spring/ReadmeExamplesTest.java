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
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.RoleAndTerm;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;
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
 * Compiles and runs the code in docs/spring-boot.md.
 *
 * <p>Same rule as {@code ReadmeExamplesTest} in the library: if the public API changes, this is
 * where the documentation breaks, and both get fixed together. Documentation that is never executed
 * is documentation that is eventually wrong.
 *
 * <p>The classes below are that page's examples, trimmed only of the surrounding prose — the
 * shapes, the method calls and the annotations are as printed.
 */
class ReadmeExamplesTest {

    @TempDir
    Path stateDirectory;

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
                .withPropertyValues(all);
    }

    @Test
    void the_quick_start_and_the_leader_only_bean_work_as_printed() {
        runner().withUserConfiguration(ReadmeBeans.class).run(context -> {
            OutboxPublisher outbox = context.getBean(OutboxPublisher.class);
            Reconciler reconciler = context.getBean(Reconciler.class);
            LeadershipListener listener = context.getBean(LeadershipListener.class);

            assertThat(outbox.isRunning())
                    .as("a gated bean is not running before the election is won")
                    .isFalse();

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(outbox.isRunning()).isTrue();
                assertThat(listener.terms()).isNotEmpty();
            });

            reconciler.reconcile();
            assertThat(reconciler.written()).isNotEmpty();
        });
    }

    @Test
    void the_manual_start_runner_works_as_printed() {
        runner("primos.lifecycle=manual").withUserConfiguration(ReadmeBeans.class).run(context -> {
            PrimosNode node = context.getBean(PrimosNode.class);
            assertThat(node.isRunning()).isFalse();

            context.getBean(JoinTheClusterWhenReady.class).run(null);

            assertThat(node.isRunning()).isTrue();
        });
    }

    // ------------------------------------------------------------------ the documented examples

    @LeaderOnly
    static class OutboxPublisher implements SmartLifecycle {

        private volatile boolean running;

        @Override
        public void start() {
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }
    }

    static class Reconciler {

        private final PrimosNode node;
        private final List<Long> written = new CopyOnWriteArrayList<>();

        Reconciler(PrimosNode node) {
            this.node = node;
        }

        void reconcile() {
            RoleAndTerm current = node.roleAndTerm();
            if (!current.isLeader()) {
                return;
            }
            written.add(current.term());
        }

        List<Long> written() {
            return written;
        }
    }

    static class JoinTheClusterWhenReady implements ApplicationRunner {

        private final PrimosNode node;

        JoinTheClusterWhenReady(PrimosNode node) {
            this.node = node;
        }

        @Override
        public void run(ApplicationArguments args) {
            node.start();
        }
    }

    static class LeadershipListener {

        private final List<Long> terms = new CopyOnWriteArrayList<>();

        @EventListener
        void onRole(PrimosEvents.RoleChanged event) {
            if (event.isLeader()) {
                terms.add(event.term());
            }
        }

        @EventListener
        void onLeader(PrimosEvents.LeaderChanged event) {
            event.leader().ifPresent(leader -> {
                if (!leader.isSelf()) {
                    leader.peer().ifPresent(peer -> forwardWorkTo(peer.host(), peer.port()));
                }
            });
        }

        @EventListener
        void onPeerDown(PrimosEvents.PeerDown event) {
            // Reachability, not membership. The cluster did not shrink and quorum is unchanged.
            Peer peer = event.peer();
            assertThat(peer.id()).isNotNegative();
        }

        private void forwardWorkTo(String host, int port) {
            // the documentation's illustration; nothing to do here
        }

        List<Long> terms() {
            return terms;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ReadmeBeans {

        @Bean
        OutboxPublisher outboxPublisher() {
            return new OutboxPublisher();
        }

        @Bean
        Reconciler reconciler(PrimosNode node) {
            return new Reconciler(node);
        }

        @Bean
        JoinTheClusterWhenReady joinTheClusterWhenReady(PrimosNode node) {
            return new JoinTheClusterWhenReady(node);
        }

        @Bean
        LeadershipListener leadershipListener() {
            return new LeadershipListener();
        }
    }
}
