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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.Peer;
import org.lolaf.primos.store.FileStateStore;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The half of this module that is not wiring: something has to run the select loop, and Spring's
 * shutdown has to be what stops it.
 *
 * <p>Both tests drive real datagrams at a server running on its own thread and never reach into it,
 * because the thing being tested is precisely that nobody has to. The shutdown one is the one that
 * matters: a witness that goes away silently leaves every two-node cluster it serves back to
 * tolerating zero failures, and the departure datagram is how they find out in a round trip instead
 * of a probe timeout.
 */
class WitnessServerLifecycleTest {

    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final String SECRET = "a-shared-cluster-secret";
    private static final Duration PATIENCE = Duration.ofSeconds(5);

    @TempDir
    Path directory;

    private ConfigurableApplicationContext context;

    @Test
    void the_server_serves_while_the_context_is_up() {
        start();

        try (WitnessTestClient voter = client()) {
            assertThat(voter.requestVote(1L, PATIENCE))
                    .as("nothing in this test drove the loop, so a reply means Spring is running it")
                    .isTrue();
        }
    }

    @Test
    void the_lifecycle_owns_the_loop_thread_and_ends_it() {
        start();
        // Held rather than looked up again afterwards: a closed context refuses getBean, and the
        // question here is about the lifecycle's own state, not the context's.
        WitnessServerLifecycle lifecycle = context.getBean(WitnessServerLifecycle.class);
        assertThat(lifecycle.isRunning()).isTrue();

        context.close();

        assertThat(lifecycle.isRunning())
                .as("and it reports the fact rather than the intention: the thread is gone")
                .isFalse();
    }

    /**
     * What a {@code SIGTERM} does, as far as this process can tell: Spring closes the context,
     * which stops the loop and then closes the server.
     */
    @Test
    void a_shutdown_announces_the_departure_and_releases_the_state_store() {
        start();

        try (WitnessTestClient voter = client()) {
            // The witness has to have learned this node before it can tell it anything, and a vote
            // request is what does that.
            assertThat(voter.requestVote(1L, PATIENCE)).isTrue();

            context.close();

            assertThat(voter.awaitDeparture(PATIENCE))
                    .as("a cluster whose witness has silently gone looks perfectly healthy while"
                            + " tolerating zero failures, so it is told")
                    .isTrue();
        }

        // The exclusive lock on the application's state directory is the other half of a clean
        // stop: taking it again is what proves the store was closed rather than merely abandoned.
        try (FileStateStore reopened = new FileStateStore(directory.resolve("state/orders"))) {
            assertThat(reopened.load().currentTerm())
                    .as("and the vote it granted is still on disk, which is why restarting is safe")
                    .isEqualTo(1L);
        }
    }

    // ---------------------------------------------------------------- plumbing

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        Files.createDirectories(directory.resolve("state"));
        write(directory.resolve("witness.properties"),
                "bind.host=127.0.0.1\nbind.port=0\n"
                        + "state.directory=" + directory.resolve("state") + "\n"
                        + "secret.shared.value=" + SECRET + "\n");
        write(directory.resolve("applications/orders.properties"), "");
    }

    @AfterEach
    void closeContext() {
        if (context != null && context.isActive()) {
            context.close();
        }
    }

    private void start() {
        context = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .bannerMode(org.springframework.boot.Banner.Mode.OFF)
                .properties("primos.witness.config-directory=" + directory,
                        "primos.witness.node-id=" + WITNESS_ID)
                .run();
    }

    private WitnessTestClient client() {
        return new WitnessTestClient("orders", 1, SECRET, WITNESS_ID,
                context.getBean(WitnessServer.class).boundAddress());
    }

    private void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(WitnessAutoConfiguration.class)
    static class TestApplication {
    }
}
