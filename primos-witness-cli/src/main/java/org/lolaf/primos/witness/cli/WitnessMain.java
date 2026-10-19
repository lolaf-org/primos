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
package org.lolaf.primos.witness.cli;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.Peer;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.WitnessServer;
import picocli.CommandLine;
import picocli.CommandLine.*;
import picocli.CommandLine.Model.CommandSpec;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The process: parse a handful of arguments, open the configuration, serve until told to stop.
 *
 * <p>Deliberately thin. Everything an operator can get wrong is checked in
 * {@link FileConfigStore} or {@link WitnessServer}, and both fail loudly at startup rather than
 * later, because the one thing worse than a witness that will not start is a witness that starts
 * and quietly is not doing its job.
 *
 * <p>The two exit codes are the ones an init system reads: <strong>2</strong> for anything the
 * operator typed or configured wrongly — reported as a sentence and the usage text, never a stack
 * trace — and <strong>1</strong> for a failure to start, which is logged with its cause because
 * that one needs the detail.
 */
@Command(
        name = "primos-witness",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        sortSynopsis = false,
        versionProvider = WitnessMain.BuildVersion.class,
        description = {
                "A leader-ineligible voter, deployed in a third failure domain, that lets a",
                "two-node active/passive pair survive a partition without split brain. One",
                "instance serves many clusters at once, keyed by application name."},
        footer = {
                "",
                "The configuration directory holds witness.properties and an applications/",
                "directory with one <name>.properties file per served cluster. Both are re-read",
                "while the server runs: onboarding an application never needs a restart, and a",
                "restart is the one genuinely risky operation here, because while the witness is",
                "down every two-node cluster it serves tolerates zero failures."})
@Slf4j
public final class WitnessMain implements Callable<Integer> {

    /**
     * A {@code String} rather than a {@link Path}, because it is also the default picocli prints
     * in the usage text and an annotation takes only constants.
     */
    private static final String DEFAULT_CONFIG_DIRECTORY = "/etc/primos-witness";

    /**
     * How long a stop waits for the loop to close everything before giving up on it.
     */
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    @Option(names = "--config", paramLabel = "<directory>", defaultValue = DEFAULT_CONFIG_DIRECTORY,
            description = "Configuration directory. Default: ${DEFAULT-VALUE}")
    private Path configDirectory;

    @Option(names = "--node-id", paramLabel = "<id>", defaultValue = "" + Peer.MAX_ID,
            converter = NodeIdConverter.class,
            description = {
                    "This witness's node id, " + Peer.MIN_ID + "-" + Peer.MAX_ID
                            + ". Default: ${DEFAULT-VALUE}",
                    "Every cluster must configure the same id for it, and it is durable in each"
                            + " application's recorded vote, so treat it as fixed once deployed."})
    private int nodeId;

    @Option(names = "--bind", paramLabel = "<host:port>", converter = HostAndPortConverter.class,
            description = "Overrides bind.host and bind.port from the configuration.")
    private InetSocketAddress bindAddress;

    @Spec
    private CommandSpec spec;

    WitnessMain() {
    }

    public static void main(String[] args) {
        int status;
        try {
            status = new CommandLine(new WitnessMain())
                    .setExecutionExceptionHandler(new StartupFailure())
                    .execute(args);
        } catch (Error e) {
            // The handler above only sees Exception, and an Error escaping here would leave the
            // process with a bare stack trace and no line anywhere saying the witness never
            // started. An operator reading a log after the fact needs that sentence.
            log.error("the witness could not start", e);
            status = ExitCode.SOFTWARE;
        }
        System.exit(status);
    }

    /**
     * Asks the server to stop and waits for its own thread to finish closing.
     *
     * <p>Bounded, because a shutdown that hangs is worse than one that is a little abrupt: state
     * is fsynced before it is acted on, so an exit that gives up here loses nothing that had been
     * promised.
     */
    private static void stopAndAwait(WitnessServer server, CountDownLatch closed) {
        server.stop();
        try {
            closed.await(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void removeHook(Thread shutdown) {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdown);
        } catch (IllegalStateException alreadyShuttingDown) {
            // The JVM is already running its hooks, so this one cannot be removed. Nothing to do.
        }
    }

    /**
     * The help text, rendered from the same command definition the process runs.
     */
    static String usage() {
        return new CommandLine(new WitnessMain()).getUsageMessage(CommandLine.Help.Ansi.OFF);
    }

    @Override
    public Integer call() {
        if (!Files.isDirectory(configDirectory)) {
            // Thrown as a parameter problem rather than a runtime one so it is reported the way
            // a mistyped option is: one sentence, then the usage text, then exit 2. A stack trace
            // would bury the only line that matters.
            throw new ParameterException(spec.commandLine(),
                    "no configuration directory at " + configDirectory + "; pass --config <directory>");
        }
        run();
        return ExitCode.OK;
    }

    private void run() {
        WitnessServer.Builder builder = WitnessServer.builder()
                .configStore(new FileConfigStore(configDirectory, MonotonicClock.SYSTEM))
                .nodeId(nodeId);
        if (bindAddress != null) {
            builder.bindAddress(bindAddress);
        }

        CountDownLatch closed = new CountDownLatch(1);
        try (WitnessServer server = builder.build()) {
            // The hook asks the loop to finish its turn and waits for the server to be closed,
            // rather than closing anything itself: closing a state store from the signal thread
            // while the server thread is mid-write is the corruption the store exists to prevent,
            // and the JVM exits the moment its hooks return. It waits on a latch, not on the main
            // thread, because main then calls System.exit, which blocks forever during shutdown.
            Thread shutdown = new Thread(() -> stopAndAwait(server, closed), "primos-witness-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdown);
            try {
                server.run();
            } finally {
                removeHook(shutdown);
            }
        } finally {
            closed.countDown();
        }
    }

    Path configDirectory() {
        return configDirectory;
    }

    int nodeId() {
        return nodeId;
    }

    InetSocketAddress bindAddress() {
        return bindAddress;
    }

    /**
     * Turns a failure to start into a logged cause and exit code 1.
     *
     * <p>picocli's default would print a stack trace to stderr. This one is what the operator
     * finds in the log afterwards, which is where a startup failure is actually read.
     */
    static final class StartupFailure implements IExecutionExceptionHandler {

        @Override
        public int handleExecutionException(Exception e, CommandLine command, ParseResult parsed) {
            log.error("the witness could not start", e);
            return ExitCode.SOFTWARE;
        }
    }

    /**
     * Reports the version from the jar manifest, so an operator can answer "what is this box
     * running?" without unpacking anything.
     */
    static final class BuildVersion implements IVersionProvider {

        @Override
        public String[] getVersion() {
            String version = WitnessMain.class.getPackage().getImplementationVersion();
            return new String[]{
                    "primos-witness " + (version == null ? "(development build)" : version)};
        }
    }

    /**
     * A node id that is checked against the range the wire and the logs assume, at the moment it
     * is typed rather than several layers down.
     */
    static final class NodeIdConverter implements ITypeConverter<Integer> {

        @Override
        public Integer convert(String value) {
            int id;
            try {
                id = Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                throw new TypeConversionException("must be a number, but was '" + value + "'");
            }
            try {
                return Peer.requireValidId(id, "--node-id");
            } catch (IllegalArgumentException e) {
                throw new TypeConversionException(e.getMessage());
            }
        }
    }

    /**
     * {@code host:port}, or {@code :port} for the wildcard address.
     *
     * <p>Split on the <em>last</em> colon, so a bare IPv6 address is not mistaken for a
     * host-and-port pair on its first one.
     */
    static final class HostAndPortConverter implements ITypeConverter<InetSocketAddress> {

        @Override
        public InetSocketAddress convert(String value) {
            int colon = value.lastIndexOf(':');
            if (colon < 0) {
                throw new TypeConversionException("must be host:port, but was '" + value + "'");
            }
            String host = value.substring(0, colon);
            int port;
            try {
                port = Integer.parseInt(value.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new TypeConversionException(
                        "port must be a number, but was '" + value.substring(colon + 1) + "'");
            }
            if (port < 1 || port > 65_535) {
                throw new TypeConversionException("port must be 1-65535, but was " + port);
            }
            return host.isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
        }
    }
}
