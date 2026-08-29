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
package org.lolaf.primos.witness;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.lolaf.primos.Peer;
import org.lolaf.primos.time.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The process: parse a handful of arguments, open the configuration, serve until told to stop.
 *
 * <p>Deliberately thin. Everything an operator can get wrong is checked in
 * {@link FileConfigStore} or {@link WitnessServer}, and both fail loudly at startup rather than
 * later, because the one thing worse than a witness that will not start is a witness that starts
 * and quietly is not doing its job.
 */
public final class WitnessMain {

    private static final Logger log = LoggerFactory.getLogger(WitnessMain.class);

    private static final Path DEFAULT_CONFIG_DIRECTORY = Paths.get("/etc/primos-witness");

    /** How long a stop waits for the loop to close everything before giving up on it. */
    private static final java.time.Duration SHUTDOWN_TIMEOUT = java.time.Duration.ofSeconds(10);

    private WitnessMain() {
    }

    public static void main(String[] args) {
        try {
            Arguments arguments = Arguments.parse(args);
            if (arguments.help) {
                System.out.println(usage());
                return;
            }
            run(arguments);
        } catch (IllegalArgumentException e) {
            // Configuration and argument mistakes, which are the operator's to fix. The stack
            // trace would bury the one sentence that matters.
            System.err.println("primos-witness: " + e.getMessage());
            System.err.println();
            System.err.println(usage());
            System.exit(2);
        } catch (RuntimeException | Error e) {
            log.error("the witness could not start", e);
            System.exit(1);
        }
    }

    private static void run(Arguments arguments) {
        if (!Files.isDirectory(arguments.configDirectory)) {
            throw new IllegalArgumentException(
                    "no configuration directory at " + arguments.configDirectory
                            + "; pass --config <directory>");
        }

        WitnessServer.Builder builder = WitnessServer.builder()
                .configStore(new FileConfigStore(arguments.configDirectory, MonotonicClock.SYSTEM))
                .nodeId(arguments.nodeId);
        if (arguments.bindAddress != null) {
            builder.bindAddress(arguments.bindAddress);
        }

        try (WitnessServer server = builder.build()) {
            // The hook asks the loop to finish its turn and then waits for it, rather than closing
            // anything itself. Two reasons, and both are the difference between a clean stop and a
            // torn one. Closing a state store from the signal thread while the server thread is
            // mid-write is exactly the corruption the store exists to prevent. And the JVM exits
            // the moment its hooks return — without the join, SIGTERM would cut the server thread
            // off before it had closed anything at all.
            Thread serverThread = Thread.currentThread();
            Thread shutdown = new Thread(() -> stopAndWaitFor(server, serverThread),
                    "primos-witness-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdown);
            try {
                server.run();
            } finally {
                removeHook(shutdown);
            }
        }
    }

    /**
     * Asks the server to stop and waits for its own thread to finish closing.
     *
     * <p>Bounded, because a shutdown that hangs is worse than one that is a little abrupt: state
     * is fsynced before it is acted on, so an exit that gives up here loses nothing that had been
     * promised.
     */
    private static void stopAndWaitFor(WitnessServer server, Thread serverThread) {
        server.stop();
        try {
            serverThread.join(SHUTDOWN_TIMEOUT.toMillis());
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

    static String usage() {
        return String.join(System.lineSeparator(),
                "usage: primos-witness [options]",
                "",
                "  --config <directory>   configuration directory (default "
                        + DEFAULT_CONFIG_DIRECTORY + ")",
                "  --node-id <id>         this witness's node id, " + Peer.MIN_ID + "-" + Peer.MAX_ID
                        + " (default " + Peer.MAX_ID + ").",
                "                         Every cluster must configure the same id for it, and it",
                "                         is durable in each application's recorded vote, so treat",
                "                         it as fixed once deployed.",
                "  --bind <host:port>     overrides bind.host and bind.port from the configuration",
                "  --help                 this text",
                "",
                "The configuration directory holds witness.properties and an applications/",
                "directory with one <name>.properties file per served cluster. Both are re-read",
                "while the server runs: onboarding an application never needs a restart, and a",
                "restart is the one genuinely risky operation here, because while the witness is",
                "down every two-node cluster it serves tolerates zero failures.");
    }

    /** The parsed command line. */
    static final class Arguments {

        private Path configDirectory = DEFAULT_CONFIG_DIRECTORY;
        private int nodeId = Peer.MAX_ID;
        private InetSocketAddress bindAddress;
        private boolean help;

        static Arguments parse(String[] args) {
            Arguments parsed = new Arguments();
            for (int i = 0; i < args.length; i++) {
                String argument = args[i];
                switch (argument) {
                    case "--help":
                    case "-h":
                        parsed.help = true;
                        break;
                    case "--config":
                        parsed.configDirectory = Paths.get(value(args, ++i, argument));
                        break;
                    case "--node-id":
                        parsed.nodeId = Peer.requireValidId(
                                number(value(args, ++i, argument), argument), "--node-id");
                        break;
                    case "--bind":
                        parsed.bindAddress = address(value(args, ++i, argument));
                        break;
                    default:
                        throw new IllegalArgumentException("unknown option '" + argument + "'");
                }
            }
            return parsed;
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

        boolean isHelp() {
            return help;
        }

        private static String value(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException(option + " needs a value");
            }
            return args[index];
        }

        private static int number(String raw, String option) {
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(option + " must be a number, but was '" + raw + "'");
            }
        }

        private static InetSocketAddress address(String raw) {
            int colon = raw.lastIndexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException("--bind must be host:port, but was '" + raw + "'");
            }
            String host = raw.substring(0, colon);
            int port = number(raw.substring(colon + 1), "--bind");
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("--bind port must be 1-65535, but was " + port);
            }
            return host.isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
        }
    }
}
