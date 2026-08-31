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

import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Timings;
import org.lolaf.primos.time.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.ArrayList;
import java.util.*;
import java.util.Comparator;
import java.util.Locale;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Configuration as a directory of properties files.
 *
 * <pre>
 * /etc/primos-witness/witness.properties
 * /etc/primos-witness/applications/orders.properties
 * /etc/primos-witness/applications/billing.properties
 * </pre>
 *
 * <p>A directory rather than one file, because this is multi-tenant infrastructure. Adding a
 * cluster is a file drop rather than an edit to a shared file every tenant contends over; each
 * team's secret can be given its own file permissions, which one shared file makes impossible; and
 * the application name comes from the <em>filename</em>, so it cannot disagree with the file's own
 * contents.
 *
 * <p>Plain {@link Properties}, so the witness distribution keeps zero dependencies. This process
 * holds cluster secrets and is meant to run untouched for years, which makes every dependency a
 * future security notification and an unplanned upgrade of something whose job is never to move.
 */
public final class FileConfigStore implements ConfigStore {


    private static final Logger log = LoggerFactory.getLogger(FileConfigStore.class);

    private static final String APPLICATIONS_DIRECTORY = "applications";
    private static final String SETTINGS_FILE = "witness.properties";
    private static final String SUFFIX = ".properties";

    private final Path directory;
    private final Path applicationsDirectory;
    private final List<ConfigChangeListener> listeners = new CopyOnWriteArrayList<>();

    private WatchService watcher;
    private volatile ConfigSnapshot current;
    private long reloadDueNanos = Long.MAX_VALUE;
    private long nextRescanNanos;
    private boolean closed;

    /**
     * Reads the directory once and starts watching it.
     *
     * <p>The clock is read here and not kept: every later question about time arrives as an
     * argument, because {@link ConfigStore#poll(long)} is called from the server's own loop and
     * takes that loop's reading. Only the first rescan deadline has nobody to be given one by.
     *
     * @throws IllegalArgumentException if the initial configuration cannot be read at all — a
     *                                  server that cannot be configured must not start
     */
    public FileConfigStore(Path directory, MonotonicClock clock) {
        this.directory = directory;
        this.applicationsDirectory = directory.resolve(APPLICATIONS_DIRECTORY);
        this.current = read();
        this.nextRescanNanos = clock.nanoTime() + current.settings().rescanInterval().toNanos();
        startWatching();
        logServedApplications();
    }

    /**
     * The intervals the served nodes run with: <strong>either</strong> a named profile
     * <strong>or</strong> the values spelled out, never both.
     *
     * <p>A profile is a set of intervals chosen so the relationships between them hold — the
     * election timeout an order of magnitude above the round trip, and so on. Overriding part of
     * one leaves a custom set whose author has not seen the other values, so the mixture is refused
     * rather than merged.
     *
     * <p><strong>Whatever is set here must match the clusters this witness serves.</strong> A node
     * refuses a pre-vote while it has heard from a leader within {@code electionTimeoutMin}, so a
     * witness with a longer one is proportionally more reluctant to agree to the failover its vote
     * is needed for — and for a two-node pair the witness's vote is the one everything waits on.
     * Nothing detects the mismatch; failover just gets slower.
     *
     * <p>Durations are milliseconds rather than {@code 250ms} strings, because this file is parsed
     * with {@link Properties} and nothing else, and a duration parser is not worth writing twice.
     */
    private static Timings timings(Properties properties) {
        String profile = properties.getProperty("timings.profile");
        List<String> explicit = new ArrayList<>(5);
        Integer heartbeat = optionalInt(properties, "timings.heartbeat.millis", explicit);
        Integer electionMin = optionalInt(properties, "timings.election.min.millis", explicit);
        Integer electionMax = optionalInt(properties, "timings.election.max.millis", explicit);
        Integer probe = optionalInt(properties, "timings.probe.millis", explicit);
        Integer advertisement = optionalInt(properties, "timings.advertisement.millis", explicit);
        Integer failureThreshold = optionalInt(properties, "timings.failure.threshold", explicit);

        if (profile != null && !profile.trim().isEmpty() && !explicit.isEmpty()) {
            throw new IllegalArgumentException("timings.profile is set to '" + profile.trim()
                    + "' and so is " + String.join(", ", explicit) + ". Choose one: a profile is a"
                    + " set of intervals picked to hold together, so overriding part of one leaves"
                    + " a custom set whose author has not seen the other values. Either drop"
                    + " timings.profile and name every interval, or drop the individual keys.");
        }
        if (profile != null && !profile.trim().isEmpty()) {
            return timingsProfile(profile);
        }
        if (explicit.isEmpty()) {
            return Timings.lan();
        }
        // The two bounds are a pair. Timings.Builder deliberately offers no setter for either
        // alone, because setting one against the other's default is how an invalid range is built.
        if ((electionMin == null) != (electionMax == null)) {
            throw new IllegalArgumentException("timings.election."
                    + (electionMin != null ? "min" : "max") + ".millis is set without its pair. The"
                    + " election timeout is a range drawn from afresh on every reset, and setting"
                    + " one end against the other's default is how an invalid or too-narrow range"
                    + " is built. Set both.");
        }
        Timings.Builder builder = Timings.builder();
        if (heartbeat != null) {
            builder.heartbeatInterval(Duration.ofMillis(heartbeat));
        }
        if (electionMin != null) {
            builder.electionTimeout(Duration.ofMillis(electionMin), Duration.ofMillis(electionMax));
        }
        if (probe != null) {
            builder.probeInterval(Duration.ofMillis(probe));
        }
        if (advertisement != null) {
            builder.advertisementInterval(Duration.ofMillis(advertisement));
        }
        if (failureThreshold != null) {
            builder.failureThreshold(failureThreshold);
        }
        // Every relationship between these is validated in here, with its own message.
        return builder.build();
    }

    private static Timings timingsProfile(String profile) {
        String name = profile.trim().toLowerCase(Locale.ROOT);
        if ("lan".equals(name)) {
            return Timings.lan();
        }
        if ("wan".equals(name)) {
            return Timings.wan();
        }
        throw new IllegalArgumentException(
                "timings.profile must be 'lan' or 'wan', but was '" + profile + "'");
    }

    /**
     * Reads an optional integer, recording the key in {@code explicit} when it was actually set —
     * which is the question the exclusivity check has to answer, and the reason this cannot simply
     * default.
     */
    private static Integer optionalInt(Properties properties, String key, List<String> explicit) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        explicit.add(key);
        try {
            return Integer.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a whole number of "
                    + (key.endsWith(".millis") ? "milliseconds" : "probes") + ", but was '"
                    + raw.trim() + "'", e);
        }
    }

    /**
     * Anything but an explicit {@code false} is true, because a setting that is on by default
     * should not be turned off by a typo.
     */
    private static boolean booleanValue(Properties properties, String key, boolean fallback) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return fallback;
        }
        return !"false".equalsIgnoreCase(value.trim());
    }

    private static int intValue(Properties properties, String key, int fallback) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a number, but was '" + raw + "'");
        }
    }

    /**
     * A secret given inline, as {@code env:NAME}, or as {@code file:path}.
     *
     * <p>The indirections exist so configuration can go into version control without the secret
     * going with it.
     */
    private static byte[] secretValue(Properties properties, String key, Path base) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        String value = raw.trim();
        if (value.startsWith("env:")) {
            String variable = value.substring(4);
            String fromEnvironment = System.getenv(variable);
            if (fromEnvironment == null || fromEnvironment.isEmpty()) {
                throw new IllegalArgumentException(
                        key + " refers to environment variable " + variable + ", which is not set");
            }
            return fromEnvironment.getBytes(StandardCharsets.UTF_8);
        }
        if (value.startsWith("file:")) {
            Path path = base.getFileSystem().getPath(value.substring(5));
            try {
                return Files.readAllBytes(path);
            } catch (IOException e) {
                throw new IllegalArgumentException(key + " refers to " + path + ", which cannot be"
                        + " read: " + e.getMessage(), e);
            }
        }
        if (value.startsWith("base64:")) {
            return Base64.getDecoder().decode(value.substring(7));
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ConfigSnapshot current() {
        return current;
    }

    @Override
    public void addListener(ConfigChangeListener listener) {
        listeners.add(listener);
    }

    @Override
    public boolean poll(long nowNanos) {
        if (closed) {
            return false;
        }
        drainWatchEvents(nowNanos);

        boolean due = nowNanos >= reloadDueNanos || nowNanos >= nextRescanNanos;
        if (!due) {
            return false;
        }
        reloadDueNanos = Long.MAX_VALUE;
        nextRescanNanos = nowNanos + current.settings().rescanInterval().toNanos();
        return reload();
    }

    @Override
    public long nextCheckNanos(long nowNanos) {
        return Math.min(reloadDueNanos, nextRescanNanos);
    }

    @Override
    public void close() {
        closed = true;
        if (watcher != null) {
            try {
                watcher.close();
            } catch (IOException e) {
                log.debug("closing the config watcher failed", e);
            }
        }
    }

    /**
     * Re-reads everything and publishes it, unless it cannot be read.
     *
     * <p><strong>A malformed file never takes down the running configuration.</strong> The whole
     * directory is parsed into a candidate, validated, and published only if it is good; otherwise
     * the last good snapshot stays in force and the problem is logged. Someone editing a file at
     * three in the morning must not be able to unconfigure a running witness with a typo.
     */
    private boolean reload() {
        ConfigSnapshot candidate;
        try {
            candidate = read();
        } catch (RuntimeException e) {
            log.error("cannot read the configuration in {}; keeping the {} application(s) already"
                            + " being served. Fix the file and it will be picked up automatically.",
                    directory, current.applications().size(), e);
            return false;
        }
        ConfigSnapshot previous = current;
        if (previous.applications().equals(candidate.applications())) {
            return false;
        }
        current = candidate;
        announce(previous, candidate);
        return true;
    }

    private void announce(ConfigSnapshot previous, ConfigSnapshot candidate) {
        for (ApplicationConfig application : candidate.applications().values()) {
            ApplicationConfig before = previous.application(application.name());
            if (before == null) {
                log.info("now serving application '{}'", application.name());
                listeners.forEach(listener -> listener.onApplicationAdded(application));
            } else if (!before.equals(application)) {
                log.info("configuration changed for application '{}'", application.name());
                listeners.forEach(listener -> listener.onApplicationChanged(before, application));
            }
        }
        for (ApplicationConfig application : previous.applications().values()) {
            if (!candidate.serves(application.name())) {
                log.info("no longer serving application '{}'. Its state is kept: a cluster that"
                                + " returns still needs the vote it was given remembered.",
                        application.name());
                listeners.forEach(listener -> listener.onApplicationRemoved(application));
            }
        }
    }

    /**
     * Rescans the whole directory on any event rather than interpreting individual ones.
     *
     * <p>Editors and file copies produce storms of create, modify and rename events, and a
     * whole-directory rescan is both simpler and more robust than trying to reconstruct intent
     * from them. The debounce turns one save into one reload.
     */
    private void drainWatchEvents(long nowNanos) {
        if (watcher == null) {
            return;
        }
        boolean sawSomething = false;
        WatchKey key;
        while ((key = watcher.poll()) != null) {
            key.pollEvents();
            key.reset();
            sawSomething = true;
        }
        if (sawSomething) {
            Duration debounce = current.settings().reloadDebounce();
            reloadDueNanos = nowNanos + debounce.toNanos();
        }
    }

    private void startWatching() {
        try {
            watcher = directory.getFileSystem().newWatchService();
            register(directory);
            if (Files.isDirectory(applicationsDirectory)) {
                register(applicationsDirectory);
            }
        } catch (IOException | UnsupportedOperationException e) {
            log.warn("this filesystem does not support watching {}; configuration will still be"
                            + " re-read every {}s", directory,
                    current.settings().rescanInterval().getSeconds(), e);
            watcher = null;
        }
    }

    private void register(Path path) throws IOException {
        path.register(watcher,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE);
    }

    private ConfigSnapshot read() {
        WitnessSettings settings = readSettings();
        Map<String, ApplicationConfig> applications = new LinkedHashMap<>();

        if (Files.isDirectory(applicationsDirectory)) {
            List<Path> files = new ArrayList<>();
            try (DirectoryStream<Path> stream =
                         Files.newDirectoryStream(applicationsDirectory, "*" + SUFFIX)) {
                stream.forEach(files::add);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot list " + applicationsDirectory, e);
            }
            files.sort(Comparator.comparing(Path::toString));
            for (Path file : files) {
                ApplicationConfig application = readApplication(file, settings);
                applications.put(application.name(), application);
            }
        }
        return new ConfigSnapshot(settings, applications);
    }

    private WitnessSettings readSettings() {
        Properties properties = load(directory.resolve(SETTINGS_FILE), true);
        WitnessSettings.Builder builder = WitnessSettings.builder()
                .stateDirectory(directory.resolve("state"));

        String host = properties.getProperty("bind.host", "0.0.0.0");
        int port = intValue(properties, "bind.port", 7420);
        builder.bindAddress(new InetSocketAddress(host, port));

        String state = properties.getProperty("state.directory");
        if (state != null && !state.trim().isEmpty()) {
            builder.stateDirectory(directory.getFileSystem().getPath(state.trim()));
        }
        String mode = properties.getProperty("registration.mode", "strict").trim();
        builder.registrationMode(RegistrationMode.valueOf(mode.toUpperCase(Locale.ROOT)));
        builder.autoRegistrationLimit(intValue(properties, "registration.limit", 64));
        builder.defaultSecret(secretValue(properties, "secret", directory));
        String algorithm = properties.getProperty("secret.algorithm");
        if (algorithm != null && !algorithm.trim().isEmpty()) {
            builder.defaultMacAlgorithm(algorithm.trim());
        }
        builder.reloadDebounce(Duration.ofMillis(intValue(properties, "reload.debounce.millis", 250)));
        builder.rescanInterval(Duration.ofSeconds(intValue(properties, "reload.rescan.seconds", 30)));
        builder.timings(timings(properties));
        builder.voteLogging(booleanValue(properties, "vote.logging", true));
        return builder.build();
    }

    private ApplicationConfig readApplication(Path file, WitnessSettings settings) {
        String name = applicationNameOf(file);
        warnIfWorldReadable(file);
        Properties properties = load(file, false);

        byte[] secret = secretValue(properties, "secret", directory);
        byte[] previous = secretValue(properties, "secret.previous", directory);
        if (secret == null) {
            // The witness-wide secret is what makes permissive mode possible at all: an
            // application nobody configured has nowhere to have had one set.
            secret = settings.defaultSecret();
        }
        // Per application rather than per witness, because a witness serves clusters it does not
        // own: one of them changing algorithm must not be a flag day for every other tenant.
        String algorithm = properties.getProperty("secret.algorithm");
        if (algorithm == null || algorithm.trim().isEmpty()) {
            algorithm = settings.defaultMacAlgorithm();
        }
        return new ApplicationConfig(name, secret, previous, algorithm.trim(), false);
    }

    /**
     * The application name comes from the filename, and is held to the same rules as the field it
     * travels in — a name too long to fit on the wire would be served here and unreachable there.
     */
    private String applicationNameOf(Path file) {
        String fileName = file.getFileName().toString();
        String name = fileName.substring(0, fileName.length() - SUFFIX.length());
        if (name.isEmpty()) {
            throw new IllegalArgumentException("an application file needs a name: " + file);
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!ClusterConfig.isApplicationNameCharacter(c)) {
                throw new IllegalArgumentException(
                        "application name '" + name + "' from " + file + " must be printable ASCII"
                                + " without spaces, because it travels in a fixed field on every"
                                + " datagram");
            }
        }
        int bytes = name.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > ClusterConfig.APPLICATION_NAME_MAX_BYTES) {
            throw new IllegalArgumentException("application name '" + name + "' from " + file
                    + " is " + bytes + " bytes; the wire field holds "
                    + ClusterConfig.APPLICATION_NAME_MAX_BYTES);
        }
        return name;
    }

    /**
     * Warns the way {@code ssh} does, rather than refusing: the operator owns their filesystem.
     */
    private void warnIfWorldReadable(Path file) {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
            if (permissions.contains(PosixFilePermission.OTHERS_READ)) {
                log.warn("{} is world-readable and may contain a cluster secret. Anyone who can"
                        + " read it can forge messages for that cluster.", file);
            }
        } catch (IOException | UnsupportedOperationException e) {
            log.debug("cannot check permissions on {}: {}", file, e.toString());
        }
    }

    private Properties load(Path file, boolean optional) {
        Properties properties = new Properties();
        if (!Files.exists(file)) {
            if (optional) {
                return properties;
            }
            throw new IllegalArgumentException("missing configuration file " + file);
        }
        try (InputStream in = Files.newInputStream(file)) {
            properties.load(in);
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("cannot parse " + file + ": " + e.getMessage(), e);
        }
        return properties;
    }

    private void logServedApplications() {
        ConfigSnapshot snapshot = current;
        if (snapshot.applications().isEmpty()) {
            log.warn("no applications configured in {}. In strict mode this witness will serve"
                    + " nobody.", applicationsDirectory);
        } else {
            log.info("serving {} application(s): {}", snapshot.applications().size(),
                    String.join(", ", snapshot.applicationNames()));
        }
        log.info("registration mode is {}", snapshot.settings().registrationMode());
    }
}
