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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Timings;
import org.lolaf.primos.witness.ConfigStore;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Which store the application runs on, and what each one costs.
 *
 * <p>The first test is the one that matters and the reason {@link ConfigStore} is a seam at all:
 * <strong>onboarding a cluster must not need a restart</strong>, because while a witness is down
 * every two-node cluster it serves tolerates zero failures. Binding the served set from Spring
 * properties is the obvious thing to do in this packaging and would remove that silently, which is
 * why hot reload is asserted here rather than assumed to have survived the move.
 */
class ConfigStoreChoiceTest {

    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final String SECRET = "a-shared-cluster-secret";
    private static final Duration PATIENCE = Duration.ofSeconds(10);

    @TempDir
    Path directory;

    private ConfigurableApplicationContext context;

    /**
     * The box this step exists for.
     */
    @Test
    void a_cluster_can_be_onboarded_without_restarting_the_boot_application() {
        application("orders");
        start("primos.witness.config-directory=" + directory);
        WitnessServer server = context.getBean(WitnessServer.class);
        assertThat(server.servedApplications()).containsExactly("orders");

        application("billing");

        assertThat(awaitServing(server, "billing"))
                .as("a file drop, no restart — the whole reason the configuration is not bound"
                        + " from Spring properties")
                .isTrue();
        try (WitnessTestClient voter = new WitnessTestClient("billing", 1, SECRET, WITNESS_ID,
                server.boundAddress())) {
            assertThat(voter.requestVote(1L, PATIENCE))
                    .as("and it is genuinely served, not merely listed")
                    .isTrue();
        }
    }

    @Test
    void the_file_store_is_what_an_application_gets_by_default() {
        application("orders");
        start("primos.witness.config-directory=" + directory);

        assertThat(context.getBean(ConfigStore.class)).isInstanceOf(FileConfigStore.class);
    }

    // ------------------------------------------------- the opt-in properties store

    @Test
    void the_properties_store_serves_what_the_properties_say() {
        start("primos.witness.config-store=properties",
                "primos.witness.state-directory=" + directory.resolve("state"),
                "primos.witness.applications[0].name=orders",
                "primos.witness.keys.shared.value=" + SECRET,
                "primos.witness.bind.host=127.0.0.1",
                "primos.witness.bind.port=0");
        WitnessServer server = context.getBean(WitnessServer.class);

        assertThat(context.getBean(ConfigStore.class)).isInstanceOf(PropertiesConfigStore.class);
        assertThat(server.servedApplications()).containsExactly("orders");
        try (WitnessTestClient voter = new WitnessTestClient("orders", 1, SECRET, WITNESS_ID,
                server.boundAddress())) {
            assertThat(voter.requestVote(1L, PATIENCE))
                    .as("the keyring came from properties too, so a granted vote is what proves"
                            + " the whole mapping and not just the name list")
                    .isTrue();
        }
    }

    @Test
    void the_properties_store_never_reports_a_change() {
        start("primos.witness.config-store=properties",
                "primos.witness.state-directory=" + directory.resolve("state"),
                "primos.witness.applications[0].name=orders",
                "primos.witness.keys.shared.value=" + SECRET,
                "primos.witness.bind.host=127.0.0.1",
                "primos.witness.bind.port=0");
        ConfigStore store = context.getBean(ConfigStore.class);

        assertThat(store.poll(System.nanoTime()))
                .as("nothing can change, which is the cost this store is chosen for")
                .isFalse();
        assertThat(store.nextCheckNanos(0L))
                .as("far ahead, but finite: the server computes deadline - now, and nanoTime may"
                        + " be negative, where Long.MAX_VALUE would overflow into a busy loop")
                .isGreaterThan(0L)
                .isLessThan(Long.MAX_VALUE / 2);
    }

    /**
     * The intervals are the file store's, in Spring's spelling: a profile, or every value named,
     * and never a mixture — the rule that stops a half-overridden profile becoming a custom set
     * whose author never saw the other values.
     */
    @Test
    void the_properties_store_takes_a_profile_or_the_intervals_spelled_out() {
        start(propertiesStore("primos.witness.timings.heartbeat-interval=250ms",
                "primos.witness.timings.election-timeout-min=2s",
                "primos.witness.timings.election-timeout-max=4s",
                "primos.witness.timings.probe-interval=3s",
                "primos.witness.timings.failure-threshold=5"));

        Timings timings = context.getBean(ConfigStore.class).current().settings().defaultTimings();
        assertThat(timings.heartbeatInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(timings.electionTimeoutMin()).isEqualTo(Duration.ofSeconds(2));
        assertThat(timings.electionTimeoutMax()).isEqualTo(Duration.ofSeconds(4));
        assertThat(timings.probeInterval()).isEqualTo(Duration.ofSeconds(3));
        assertThat(timings.failureThreshold()).isEqualTo(5);
    }

    @Test
    void a_profile_beside_a_named_interval_is_refused_with_both_quoted_back() {
        assertThatStartupFails(propertiesStore("primos.witness.timings.profile=wan",
                "primos.witness.timings.heartbeat-interval=250ms"))
                .contains("primos.witness.timings.profile is set to 'wan'")
                .contains("primos.witness.timings.heartbeat-interval");
    }

    @Test
    void half_an_election_range_is_refused_by_the_property_that_is_missing() {
        assertThatStartupFails(propertiesStore("primos.witness.timings.election-timeout-min=2s"))
                // The message names the end that IS set, since that is the one an operator wrote
                // and can find.
                .contains("primos.witness.timings.election-timeout-min is set without its pair");
    }

    /**
     * The reason applications are objects rather than names: a witness serves clusters it does not
     * own, and one of them on another network must not decide the intervals for all of them. That
     * is why the file store keeps a file per application, and this store has to be able to say the
     * same thing.
     */
    @Test
    void one_application_may_run_its_own_intervals_while_the_rest_take_the_witness_default() {
        start(propertiesStore("primos.witness.timings.profile=lan",
                "primos.witness.applications[1].name=billing",
                "primos.witness.applications[1].timings.profile=wan"));

        var applications = context.getBean(ConfigStore.class).current();
        assertThat(applications.application("orders").timings().electionTimeoutMin())
                .as("the witness-wide default")
                .isEqualTo(Timings.lan().electionTimeoutMin());
        assertThat(applications.application("billing").timings().electionTimeoutMin())
                .as("its own, because it is the one on the far side of a WAN")
                .isEqualTo(Timings.wan().electionTimeoutMin());
    }

    /**
     * The witness-wide value is a default rather than something a served node runs, and it has two
     * consumers: an application that names no intervals of its own, and — the one that only exists
     * here — an application auto-registered under {@code config.source=cluster}, which has no file
     * to fall back to. Leaving it unset would silently give those the LAN profile instead of what
     * the operator configured.
     */
    @Test
    void the_witness_wide_default_is_carried_into_the_settings_for_auto_registered_clusters() {
        start(propertiesStore("primos.witness.config-source=cluster",
                "primos.witness.timings.profile=wan"));

        assertThat(context.getBean(ConfigStore.class).current().settings().defaultTimings()
                .electionTimeoutMin()).isEqualTo(Timings.wan().electionTimeoutMin());
    }

    @Test
    void the_properties_store_refuses_to_run_without_a_state_directory() {
        assertThatStartupFails("primos.witness.config-store=properties",
                "primos.witness.applications[0].name=orders")
                .contains("primos.witness.state-directory is required");
    }

    /**
     * The mirror of the precedence rule: what the file owns may not be bound from Spring, and
     * quietly ignoring it is how the state directory ends up somewhere nobody expects.
     */
    @Test
    void settings_the_file_owns_are_refused_under_the_file_store() {
        application("orders");
        assertThatStartupFails("primos.witness.config-directory=" + directory,
                "primos.witness.state-directory=" + directory.resolve("elsewhere"))
                .contains("primos.witness.state-directory")
                .contains("config-store=properties");
    }

    // ---------------------------------------------------------------- plumbing

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        Files.createDirectories(directory.resolve("state"));
        write(directory.resolve("witness.properties"),
                "bind.host=127.0.0.1\nbind.port=0\n"
                        + "state.directory=" + directory.resolve("state") + "\n"
                        + "secret.shared.value=" + SECRET + "\n"
                        // Faster than the shipped default so the reload test is not five seconds of
                        // waiting; the mechanism under test is the same one either way.
                        + "reload.rescan.seconds=1\n");
    }

    @AfterEach
    void closeContext() {
        if (context != null && context.isActive()) {
            context.close();
        }
    }

    /**
     * A working properties-store configuration, plus whatever a test is actually saying.
     */
    private String[] propertiesStore(String... extra) {
        List<String> properties = new ArrayList<>(List.of(
                "primos.witness.config-store=properties",
                "primos.witness.state-directory=" + directory.resolve("state"),
                "primos.witness.applications[0].name=orders",
                "primos.witness.keys.shared.value=" + SECRET,
                "primos.witness.bind.host=127.0.0.1",
                "primos.witness.bind.port=0"));
        properties.addAll(List.of(extra));
        return properties.toArray(new String[0]);
    }

    private void start(String... properties) {
        context = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .bannerMode(org.springframework.boot.Banner.Mode.OFF)
                .properties(properties)
                .run();
    }

    /**
     * The whole cause chain as one string, because Spring wraps a bean's refusal several layers
     * deep and the sentence written for the operator is at the bottom of it.
     */
    private org.assertj.core.api.AbstractStringAssert<?> assertThatStartupFails(String... properties) {
        Throwable failure = org.assertj.core.api.Assertions.catchThrowable(() -> start(properties));
        assertThat(failure).as("this configuration must not start").isNotNull();
        StringBuilder chain = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            chain.append(cause.getMessage()).append('\n');
        }
        return assertThat(chain.toString());
    }

    /** The reload runs on the server's own thread, so the change lands when it lands. */
    private boolean awaitServing(WitnessServer server, String application) {
        long deadline = System.nanoTime() + PATIENCE.toNanos();
        while (System.nanoTime() < deadline) {
            if (server.servedApplications().contains(application)) {
                return true;
            }
            Thread.onSpinWait();
        }
        return false;
    }

    private void application(String name) {
        write(directory.resolve("applications/" + name + ".properties"), "");
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
