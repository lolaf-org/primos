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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The endpoint as an operator meets it: over HTTP, from the packaged application.
 *
 * <p>Worth a test of its own rather than trusting the indicator's unit tests, because everything
 * between them and a readable answer is configuration — exposure, and whether the detail an
 * operator needs is rendered at all. {@code show-details} defaults to {@code never}, so without the
 * default this module ships, a quarantined tenant would be invisible at exactly the moment somebody
 * went looking for it.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WitnessActuatorTest {

    @TempDir
    static Path directory;

    @LocalServerPort
    private int port;

    @BeforeAll
    static void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        Files.createDirectories(directory.resolve("state"));
        Files.write(directory.resolve("witness.properties"),
                ("bind.host=127.0.0.1\nbind.port=0\n"
                        + "state.directory=" + directory.resolve("state") + "\n"
                        + "secret.shared.value=a-shared-cluster-secret\n")
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(directory.resolve("applications/orders.properties"), new byte[0]);
    }

    @DynamicPropertySource
    static void witnessConfiguration(DynamicPropertyRegistry registry) {
        registry.add("primos.witness.config-directory", directory::toString);
    }

    @Test
    void health_is_up_and_says_what_it_is_serving() {
        HttpResponse<String> response = get("/actuator/health");

        assertThat(response.statusCode()).isEqualTo(200);
        String body = response.body();
        assertThat(body).contains("\"status\":\"UP\"")
                .as("the detail is the whole value of this endpoint on a witness: which tenants it"
                        + " has, and which of them it has stopped answering for")
                .contains("\"served\":[\"orders\"]")
                .contains("\"quarantined\":[]")
                .as("nothing but the witness itself may answer whether this witness is serving:"
                        + " Boot's disk-space contributor watches a directory this process does not"
                        + " care about, and a DOWN from it would get somebody to restart the one"
                        + " thing whose restart is genuinely risky")
                .doesNotContain("diskSpace");
    }

    /**
     * <strong>Spring Boot ships this endpoint by default and this module switches it off</strong>,
     * which is why the test asserts a 404 rather than documenting one.
     *
     * <p>A Kubernetes user who finds {@code /actuator/health/liveness} will point a
     * {@code livenessProbe} at it, because that is what it is for — and on a witness that is a
     * mechanism for causing the outage it is watching for: restarting one is the risky operation in
     * its lifecycle, since while it is down every two-node cluster it serves tolerates zero
     * failures. Readiness is fine and needs no group: {@code /actuator/health} is UP exactly while
     * this witness is serving.
     */
    @Test
    void the_liveness_probe_group_is_not_shipped() {
        assertThat(get("/actuator/health/liveness").statusCode()).isEqualTo(404);
    }

    /**
     * The JDK's own client rather than a Spring test one, so this test says nothing about which
     * HTTP client happens to be on the classpath — it is about what the endpoint returns.
     */
    private HttpResponse<String> get(String path) {
        try {
            return HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
