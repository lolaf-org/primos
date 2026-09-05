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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The container image, driven the way a cluster drives it: real datagrams at a published UDP port.
 *
 * <p><strong>Bound only under {@code -Pdocker}, and guarded on the image name on top of that</strong>,
 * because {@code ./mvnw verify} has to stay green on a machine with no Docker daemon — which is the
 * same reason nothing in the reactor builds the image. The recipe is
 *
 * <pre>
 * docker build -f src/main/docker/Dockerfile -t primos-witness:it .
 * ./mvnw -Pdocker verify -Ddocker.image=primos-witness:it
 * </pre>
 *
 * <p>No Testcontainers. {@code docker run} plus the client this module already has is enough, and
 * the alternative is a large test dependency in a repository that counts them. Recorded because
 * reaching for it is the obvious move.
 *
 * <p>What is asserted is the three things the image can get wrong without the build noticing: that
 * the published port speaks UDP at all, that the vote it grants reaches the volume rather than the
 * writable layer, and that {@code SIGTERM} produces a departure rather than a kill. Each fails
 * silently in production — a witness that is never spoken to looks exactly like a witness nobody is
 * asking.
 */
@EnabledIfSystemProperty(named = WitnessImageIT.IMAGE_PROPERTY, matches = ".+",
        disabledReason = "no image named; see this class's javadoc for the recipe")
class WitnessImageIT {

    static final String IMAGE_PROPERTY = "docker.image";

    private static final String APPLICATION = "orders";
    private static final String SECRET = "an-integration-test-secret";
    private static final int WITNESS_ID = 1023;
    private static final int NODE_ID = 1;

    /**
     * Generous, because it covers pulling nothing but starting a Spring Boot application on a
     * loaded CI box — measured at ~4s, and a timeout tight enough to be interesting here would
     * only ever be flaky.
     */
    private static final Duration STARTUP = Duration.ofSeconds(90);

    /** Longer than the server's own shutdown, so a failure here means it did not shut down. */
    private static final Duration SHUTDOWN = Duration.ofSeconds(30);

    private String container;
    private Path work;

    @AfterEach
    void removeContainer() {
        if (container != null) {
            docker("rm", "-f", container);
        }
        deleteBestEffort(work);
    }

    @Test
    @DisplayName("serves a vote over the published UDP port, keeps it on the volume, and leaves cleanly")
    void servesAndStopsCleanly() throws Exception {
        work = workingDirectory();
        Path config = writeConfiguration(work);
        Path state = writableDirectory(work.resolve("state"));
        int port = freePort();

        container = "primos-witness-it-" + System.nanoTime();
        docker("run", "-d", "--name", container,
                // 127.0.0.1 so a test never publishes a witness to the network it is running on,
                // and /udp because without it the port is TCP and nothing is ever spoken to.
                "-p", "127.0.0.1:" + port + ":7420/udp",
                "-v", config + ":/etc/primos-witness:ro",
                "-v", state + ":/var/lib/primos-witness",
                // The image's documented posture, asserted by running under it: anything the
                // application needs to write outside these two mounts fails the test here rather
                // than an operator's first read-only deployment.
                "--read-only", "--tmpfs", "/tmp",
                System.getProperty(IMAGE_PROPERTY));

        try (WitnessTestClient node = new WitnessTestClient(APPLICATION, NODE_ID, SECRET, WITNESS_ID,
                new InetSocketAddress("127.0.0.1", port))) {

            Boolean granted = awaitVote(node);
            assertThat(granted)
                    // A supplier, so `docker logs` runs on the failure rather than on every pass.
                    .withFailMessage(() -> "no answer on the published UDP port within " + STARTUP
                            + "; the log was:\n" + docker("logs", container))
                    .isNotNull();
            assertThat(granted).isTrue();

            // The vote is on the host, not in a layer that `docker rm` discards. A witness that
            // forgets currentTerm/votedFor across a restart grants a second vote in a term it has
            // already voted in, which elects two leaders.
            assertThat(state.resolve(APPLICATION)).isNotEmptyDirectory();

            // Stopped without waiting, because the departure datagram is sent during the stop and
            // this thread has to be listening for it. -t is above the server's own timeout so that
            // a slow shutdown is a failure rather than a SIGKILL that looks like one.
            Process stopping = start("stop", "-t", "" + SHUTDOWN.getSeconds(), container);
            assertThat(node.awaitDeparture(SHUTDOWN))
                    .withFailMessage("no Leaving datagram; every served cluster then waits out a"
                            + " probe timeout for a peer that had already gone")
                    .isTrue();
            assertThat(stopping.waitFor(SHUTDOWN.getSeconds(), TimeUnit.SECONDS)).isTrue();
        }

        // 143 is SIGTERM: java is PID 1 and took the signal itself. 137 here would mean the stop
        // was killed, which is what an ENTRYPOINT behind a shell, or a grace period below the
        // shutdown timeout, produces.
        assertThat(docker("inspect", "-f", "{{.State.ExitCode}}", container).trim()).isEqualTo("143");
    }

    /**
     * Asks until answered rather than sleeping: the wait is for a Spring Boot startup, and the
     * datagram that proves it is over is the one this test is about.
     */
    private Boolean awaitVote(WitnessTestClient node) {
        long deadline = System.nanoTime() + STARTUP.toNanos();
        while (System.nanoTime() < deadline) {
            // The same term every time on purpose. A witness that has already voted for this
            // candidate in this term grants it again, so a retry cannot turn a granted vote into a
            // refused one — asking with a fresh term each time could.
            Boolean granted = node.requestVote(1L, Duration.ofMillis(500));
            if (granted != null) {
                return granted;
            }
        }
        return null;
    }

    /**
     * Under {@code target/} rather than a {@code @TempDir}, and that is not a preference.
     *
     * <p>A temporary directory is created mode 700, so the image's uid 1000 could not traverse into
     * the configuration mount; and the state the container writes back comes out owned by that uid,
     * which a build running as any other one cannot remove — so {@code @TempDir}'s cleanup would
     * fail the test for a reason that has nothing to do with the image. Removal here is best-effort
     * for the same reason, and {@code clean} sweeps up whatever is left.
     */
    private static Path workingDirectory() throws IOException {
        // Absolute, because a relative -v source is read as a named volume rather than a path.
        Path root = Path.of("target", "witness-image-it", Long.toString(System.nanoTime()))
                .toAbsolutePath();
        Files.createDirectories(root);
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwxr-xr-x"));
        return root;
    }

    private static void deleteBestEffort(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> tree = Files.walk(root)) {
            tree.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // Written by another uid; `clean` will get it.
                }
            });
        } catch (IOException ignored) {
            // Same.
        }
    }

    private static Path writeConfiguration(Path work) throws IOException {
        Path config = readableDirectory(work.resolve("conf"));
        Files.write(config.resolve("witness.properties"), List.of(
                "bind.host=0.0.0.0",
                "bind.port=7420",
                "state.directory=/var/lib/primos-witness",
                "config.source=file",
                "timings.profile=lan",
                "secret.it.value=" + SECRET), StandardCharsets.UTF_8);
        Path applications = readableDirectory(config.resolve("applications"));
        Files.createFile(applications.resolve(APPLICATION + ".properties"));
        return config;
    }

    /** Readable by the image's uid 1000, which is not the uid that ran the build. */
    private static Path readableDirectory(Path path) throws IOException {
        Files.createDirectories(path);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxr-xr-x"));
        return path;
    }

    /** Writable by it, because the votes go here and the whole point is that they survive. */
    private static Path writableDirectory(Path path) throws IOException {
        Files.createDirectories(path);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxrwxrwx"));
        return path;
    }

    private static int freePort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String docker(String... arguments) {
        try {
            Process process = start(arguments);
            String output = new String(process.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8);
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("docker " + String.join(" ", arguments)
                        + " did not finish");
            }
            // Checked, because the alternative is a failure ninety seconds later saying the port
            // never answered when what actually happened was that the container never started.
            if (process.exitValue() != 0) {
                throw new IllegalStateException("docker " + String.join(" ", arguments)
                        + " exited " + process.exitValue() + ":\n" + output);
            }
            return output;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static Process start(String... arguments) {
        List<String> command = new ArrayList<>(arguments.length + 1);
        command.add("docker");
        command.addAll(List.of(arguments));
        try {
            return new ProcessBuilder(command).redirectErrorStream(true).start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
