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
import org.lolaf.primos.Timings;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code primos.timings}: a named profile, or the intervals spelled out, and never both.
 */
class TimingsPropertiesTest {

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
    void nothing_configured_is_the_lan_profile() {
        runner().run(context ->
                assertThat(context.getBean(Timings.class)).isEqualTo(Timings.lan()));
    }

    @Test
    void a_named_profile_is_taken_whole() {
        runner("primos.timings.profile=wan").run(context ->
                assertThat(context.getBean(Timings.class)).isEqualTo(Timings.wan()));
    }

    @Test
    void the_intervals_can_be_spelled_out() {
        runner("primos.timings.heartbeat-interval=250ms",
                "primos.timings.election-timeout-min=2s",
                "primos.timings.election-timeout-max=4s",
                "primos.timings.probe-interval=3s",
                "primos.timings.advertisement-interval=60s",
                "primos.timings.failure-threshold=5")
                .run(context -> {
                    Timings timings = context.getBean(Timings.class);
                    assertThat(timings.advertisementInterval()).isEqualTo(Duration.ofSeconds(60));
                    assertThat(timings.heartbeatInterval()).isEqualTo(Duration.ofMillis(250));
                    assertThat(timings.electionTimeoutMin()).isEqualTo(Duration.ofSeconds(2));
                    assertThat(timings.electionTimeoutMax()).isEqualTo(Duration.ofSeconds(4));
                    assertThat(timings.probeInterval()).isEqualTo(Duration.ofSeconds(3));
                    assertThat(timings.failureThreshold()).isEqualTo(5);
                });
    }

    /**
     * A profile is a set of values chosen to hold together, so half of one is not a profile any
     * more. Refused rather than merged, and the message names both sides of the conflict.
     */
    @Test
    void the_advertisement_interval_counts_as_an_explicit_interval() {
        runner("primos.timings.profile=wan",
                "primos.timings.advertisement-interval=60s")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .hasStackTraceContaining("advertisement-interval"));
    }

    @Test
    void a_profile_and_an_explicit_interval_together_are_refused() {
        runner("primos.timings.profile=wan",
                "primos.timings.heartbeat-interval=250ms")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .hasStackTraceContaining("primos.timings.profile is set to 'wan'")
                        .hasStackTraceContaining("heartbeat-interval")
                        .hasStackTraceContaining("Choose one"));
    }

    /**
     * The bounds are a pair. {@code Timings} rejects an inverted range at {@code build()}, but a
     * message naming the property beats one naming a Java field, so this layer catches it first.
     */
    @Test
    void one_election_bound_without_its_pair_is_refused() {
        runner("primos.timings.election-timeout-min=2s")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .hasStackTraceContaining("election-timeout-min is set without its pair"));
    }

    /**
     * Partial custom is allowed, and the library's own validation is what catches an incoherent
     * result — a better error than demanding all five keys would give.
     */
    @Test
    void an_incoherent_custom_set_fails_with_the_librarys_own_message() {
        runner("primos.timings.heartbeat-interval=100ms")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .hasStackTraceContaining("electionTimeoutMin must be at least"));
    }
}
