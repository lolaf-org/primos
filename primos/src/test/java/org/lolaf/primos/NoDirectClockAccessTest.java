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
package org.lolaf.primos;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Enforces the rule that all time flows through {@code MonotonicClock}.
 *
 * <p>A grep in a test is crude, but the rule it protects is not: a stray
 * {@code System.currentTimeMillis()} would make timeouts jump when NTP corrects the wall clock,
 * and a stray {@code System.nanoTime()} would make the deterministic simulation stop being
 * deterministic. Both failures are invisible until they are expensive.
 */
class NoDirectClockAccessTest {

    private static final Path SOURCES = Paths.get("src/main/java");
    private static final String CLOCK = "org/lolaf/primos/time/MonotonicClock.java";

    @Test
    void no_source_reads_the_clock_directly_except_the_clock_itself() throws IOException {
        List<String> offenders = new ArrayList<>();

        try (Stream<Path> sources = Files.walk(SOURCES)) {
            for (Path source : (Iterable<Path>) sources.filter(p -> p.toString().endsWith(".java"))::iterator) {
                if (source.toString().replace('\\', '/').endsWith(CLOCK)) {
                    continue;
                }
                String body = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
                if (body.contains("System.nanoTime()") || body.contains("System.currentTimeMillis()")) {
                    offenders.add(SOURCES.relativize(source).toString());
                }
            }
        }

        assertThat(offenders)
                .as("all time must flow through MonotonicClock, so the simulation can virtualise it"
                        + " and so no timeout depends on a wall clock that NTP can move")
                .isEmpty();
    }
}
