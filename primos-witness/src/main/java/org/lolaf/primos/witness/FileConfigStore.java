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

import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Timings;
import org.lolaf.primos.time.MonotonicClock;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.*;
import java.util.zip.CRC32;

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
@Slf4j
public final class FileConfigStore implements ConfigStore {

    private static final String APPLICATIONS_DIRECTORY = "applications";
    private static final String SETTINGS_FILE = "witness.properties";
    private static final String SUFFIX = ".properties";
    private static final String SECRET_PREFIX = "secret.";
    private static final String FILE_SECRET = "file:";
    private static final String ENV_SECRET = "env:";
    private static final String HEARTBEAT_KEY = "timings.heartbeat.millis";
    private static final String ELECTION_MIN_KEY = "timings.election.min.millis";
    private static final String ELECTION_MAX_KEY = "timings.election.max.millis";
    private static final String PROBE_KEY = "timings.probe.millis";
    private static final String ADVERTISEMENT_KEY = "timings.advertisement.millis";
    private static final String FAILURE_THRESHOLD_KEY = "timings.failure.threshold";
    private static final List<String> INTERVAL_KEYS = Collections.unmodifiableList(Arrays.asList(
            HEARTBEAT_KEY, ELECTION_MIN_KEY, ELECTION_MAX_KEY, PROBE_KEY, ADVERTISEMENT_KEY, FAILURE_THRESHOLD_KEY));

    private final Path directory;
    private final Path applicationsDirectory;

    /**
     * Names already told that their timing keys are being ignored, so it is said once each.
     *
     * <p>Unbounded is safe here and nowhere else in this estate: these are filenames in a
     * directory an operator owns, not names arriving from the network.
     */
    private final Set<String> warnedIgnoredTimings = new HashSet<>();

    /**
     * A checksum of the configuration as last read. A tick that produces the same one does nothing.
     */
    private long lastChecksum;
    private volatile ConfigSnapshot current;
    private long nextRescanNanos;
    private boolean closed;

    /**
     * Reads the directory once and starts watching it.
     *
     * <p>The clock is read here and not kept: every later question about time arrives as an
     * argument, because {@link ConfigStore#poll(long)} is called from the server's own loop and
     * takes that loop's reading. Only the first rescan deadline has nobody to be given one by.
     *
     * @throws IllegalArgumentException if the initial configuration cannot be read at all: a
     *                                  server that cannot be configured must not start
     */
    public FileConfigStore(Path directory, MonotonicClock clock) {
        this.directory = directory;
        this.applicationsDirectory = directory.resolve(APPLICATIONS_DIRECTORY);
        // Before the read, so an edit landing between the two is seen by the next rescan rather than absorbed.
        this.lastChecksum = checksum();
        this.current = read();
        this.nextRescanNanos = clock.nanoTime() + current.settings().rescanInterval().toNanos();
        logServedApplications();
    }

    private static Timings timings(Properties properties, Timings fallback) {
        return Timings.resolve("timings.profile", properties.getProperty("timings.profile"),
                intervalKeysSet(properties), fallback,
                () -> Timings.builder()
                        .heartbeatIntervalIfSet(optionalMillis(properties, HEARTBEAT_KEY))
                        .electionTimeoutIfSet(optionalMillis(properties, ELECTION_MIN_KEY),
                                optionalMillis(properties, ELECTION_MAX_KEY), ELECTION_MIN_KEY, ELECTION_MAX_KEY)
                        .probeIntervalIfSet(optionalMillis(properties, PROBE_KEY))
                        .advertisementIntervalIfSet(optionalMillis(properties, ADVERTISEMENT_KEY))
                        .failureThresholdIfSet(optionalInt(properties, FAILURE_THRESHOLD_KEY))
                        .build());
    }

    private static List<String> intervalKeysSet(Properties properties) {
        List<String> set = new ArrayList<>(INTERVAL_KEYS.size());
        for (String key : INTERVAL_KEYS) {
            String raw = properties.getProperty(key);
            if (raw != null && !raw.trim().isEmpty()) {
                set.add(key);
            }
        }
        return set;
    }

    private static Integer optionalInt(Properties properties, String key) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a whole number of "
                    + (key.endsWith(".millis") ? "milliseconds" : "probes") + ", but was '"
                    + raw.trim() + "'", e);
        }
    }

    private static Duration optionalMillis(Properties properties, String key) {
        Integer millis = optionalInt(properties, key);
        return millis == null ? null : Duration.ofMillis(millis);
    }

    private static boolean trueUnlessFalse(Properties properties, String key) {
        String value = properties.getProperty(key);
        return value == null || !"false".equalsIgnoreCase(value.trim());
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

    private static Duration rescanInterval(Properties properties) {
        int seconds = intValue(properties, "reload.rescan.seconds", 5);
        if (seconds < 1) {
            throw new IllegalArgumentException("reload.rescan.seconds must be at least 1, but was " + seconds
                    + ": the check runs on the witness's own loop, which would then do nothing else");
        }
        return Duration.ofSeconds(seconds);
    }

    private static List<String> splitPatterns(String raw) {
        return raw == null ? Collections.emptyList() : Arrays.asList(raw.split(","));
    }

    private byte[] secretValue(Properties properties, String key) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        String value = raw.trim();
        if (value.startsWith(ENV_SECRET)) {
            String variable = value.substring(ENV_SECRET.length());
            String fromEnvironment = System.getenv(variable);
            if (fromEnvironment == null || fromEnvironment.isEmpty()) {
                throw new IllegalArgumentException(
                        key + " refers to environment variable " + variable + ", which is not set");
            }
            return fromEnvironment.getBytes(StandardCharsets.UTF_8);
        }
        if (value.startsWith(FILE_SECRET)) {
            Path path = secretFile(value);
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

    private Path secretFile(String value) {
        return directory.resolve(value.substring(FILE_SECRET.length()));
    }

    private static void absorb(CRC32 sum, Path file) {
        sum.update(file.getFileName().toString().getBytes(StandardCharsets.UTF_8));
        try {
            // Read through the symlink, which is what makes a ConfigMap work: after a ..data swap
            // the resolved file is a different inode, and its bytes are what this is asking about.
            sum.update(Files.readAllBytes(file));
        } catch (IOException e) {
            // Absent or unreadable sums differently from present, which is the whole requirement.
            sum.update('!');
        }
    }

    private static void refuseKeysHere(String name, Properties properties) {
        for (String property : properties.stringPropertyNames()) {
            if (property.equals("secret") || property.startsWith(SECRET_PREFIX)) {
                throw new IllegalArgumentException("application '" + name + "' sets '" + property
                        + "', but keys live in witness.properties as secret.<name>.value, with an"
                        + " optional secret.<name>.applications naming what each covers. Leaving"
                        + " this here would mean believing this tenant has its own key when the"
                        + " keyring decides otherwise.");
            }
        }
    }

    private Keyring readKeyring(Properties properties) {
        List<WitnessKey> keys = new ArrayList<>();
        for (String name : keyNames(properties)) {
            keys.add(readKey(properties, name));
        }
        return new Keyring(keys);
    }

    // Sorted, so a configuration does not read differently from one run to the next.
    private static Set<String> keyNames(Properties properties) {
        Set<String> names = new TreeSet<>();
        for (String property : properties.stringPropertyNames()) {
            if (!property.startsWith(SECRET_PREFIX)) {
                continue;
            }
            String rest = property.substring(SECRET_PREFIX.length());
            int dot = rest.indexOf('.');
            if (dot <= 0) {
                throw new IllegalArgumentException("'" + property + "' names no key: a secret is"
                        + " configured as secret.<name>.value, optionally with"
                        + " secret.<name>.algorithm and secret.<name>.applications");
            }
            names.add(rest.substring(0, dot));
        }
        return names;
    }

    private WitnessKey readKey(Properties properties, String name) {
        byte[] secret = secretValue(properties, SECRET_PREFIX + name + ".value");
        if (secret == null) {
            throw new IllegalArgumentException("key '" + name + "' has no "
                    + SECRET_PREFIX + name + ".value; a key without a secret cannot verify"
                    + " anything and is more likely a typo than an intention");
        }
        return new WitnessKey(name, secret,
                properties.getProperty(SECRET_PREFIX + name + ".algorithm"),
                splitPatterns(properties.getProperty(SECRET_PREFIX + name + ".applications")));
    }

    @Override
    public ConfigSnapshot current() {
        return current;
    }

    @Override
    public boolean poll(long nowNanos) {
        if (closed || nowNanos < nextRescanNanos) {
            return false;
        }
        nextRescanNanos = nowNanos + current.settings().rescanInterval().toNanos();

        long seen = checksum();
        if (seen == lastChecksum) {
            // The common case by an enormous margin, and it costs a checksum rather than rebuilding
            // and comparing a whole configuration to discover nothing moved.
            return false;
        }
        lastChecksum = seen;
        return reload();
    }

    @Override
    public long nextCheckNanos(long nowNanos) {
        return nextRescanNanos;
    }

    @Override
    public void close() {
        closed = true;
    }

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
        // Both halves: an edit to witness.properties alone changes no application.
        boolean settingsChanged = !previous.settings().equals(candidate.settings());
        boolean applicationsChanged = !previous.applications().equals(candidate.applications());
        if (!settingsChanged && !applicationsChanged) {
            return false;
        }
        current = candidate;
        if (settingsChanged) {
            reportRestartRequired(previous.settings(), candidate.settings());
        }
        if (applicationsChanged) {
            logApplicationChanges(previous, candidate);
        }
        return true;
    }

    private void reportRestartRequired(WitnessSettings previous, WitnessSettings candidate) {
        warnIfChanged("bind.host/bind.port", previous.bindAddress(), candidate.bindAddress());
        warnIfChanged("state.directory", previous.stateDirectory(), candidate.stateDirectory());
        warnIfChanged("vote.logging", previous.voteLogging(), candidate.voteLogging());
    }

    private void warnIfChanged(String key, Object previous, Object candidate) {
        if (!Objects.equals(previous, candidate)) {
            log.warn("{} changed from {} to {} and was read but not applied; it takes effect when"
                    + " the witness restarts", key, previous, candidate);
        }
    }

    private void logApplicationChanges(ConfigSnapshot previous, ConfigSnapshot candidate) {
        for (ApplicationConfig application : candidate.applications().values()) {
            ApplicationConfig before = previous.application(application.name());
            if (before == null) {
                log.info("now serving application '{}'", application.name());
            } else if (!before.equals(application)) {
                log.info("configuration changed for application '{}'", application.name());
            }
        }
        for (ApplicationConfig application : previous.applications().values()) {
            if (!candidate.serves(application.name())) {
                log.info("no longer serving application '{}'. Its state is kept: a cluster that"
                                + " returns still needs the vote it was given remembered.",
                        application.name());
            }
        }
    }

    private long checksum() {
        CRC32 sum = new CRC32();
        absorb(sum, directory.resolve(SETTINGS_FILE));
        currentSecretFiles().forEach(file -> absorb(sum, file));
        try {
            applicationFiles().forEach(file -> absorb(sum, file));
        } catch (IOException e) {
            // Unreadable is a state like any other: it sums differently from readable, so it
            // reloads once and the existing keep-the-last-good-configuration path reports it.
            sum.update('?');
        }
        return sum.getValue();
    }

    private List<Path> applicationFiles() throws IOException {
        if (!Files.isDirectory(applicationsDirectory)) {
            return Collections.emptyList();
        }
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(applicationsDirectory, "*" + SUFFIX)) {
            stream.forEach(files::add);
        }
        files.sort(Comparator.comparing(Path::toString));
        return files;
    }

    private List<Path> currentSecretFiles() {
        try {
            return secretFiles(load(directory.resolve(SETTINGS_FILE), true));
        } catch (IllegalArgumentException malformed) {
            // The file's own bytes are already in the sum, and the read that follows reports this.
            return Collections.emptyList();
        }
    }

    private List<Path> secretFiles(Properties properties) {
        List<Path> files = new ArrayList<>();
        for (String key : new TreeSet<>(properties.stringPropertyNames())) {
            String value = properties.getProperty(key).trim();
            if (isSecretValue(key) && value.startsWith(FILE_SECRET)) {
                files.add(secretFile(value));
            }
        }
        return files;
    }

    private static boolean isSecretValue(String key) {
        return key.startsWith(SECRET_PREFIX) && key.endsWith(".value");
    }

    private static boolean holdsInlineSecret(Properties properties) {
        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key).trim();
            if (isSecretValue(key) && !value.startsWith(FILE_SECRET) && !value.startsWith(ENV_SECRET)) {
                return true;
            }
        }
        return false;
    }

    private void warnAboutReadableSecrets(Properties properties) {
        if (holdsInlineSecret(properties)) {
            warnIfWorldReadable(directory.resolve(SETTINGS_FILE));
        }
        secretFiles(properties).forEach(this::warnIfWorldReadable);
    }

    private ConfigSnapshot read() {
        WitnessSettings settings = readSettings();
        Map<String, ApplicationConfig> applications = new LinkedHashMap<>();
        try {
            for (Path file : applicationFiles()) {
                ApplicationConfig application = readApplication(file, settings);
                applications.put(application.name(), application);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot list " + applicationsDirectory, e);
        }
        return new ConfigSnapshot(settings, applications);
    }

    private WitnessSettings readSettings() {
        Properties properties = load(directory.resolve(SETTINGS_FILE), true);
        WitnessSettings.WitnessSettingsBuilder builder = WitnessSettings.builder()
                .stateDirectory(directory.resolve("state"));

        String host = properties.getProperty("bind.host", "0.0.0.0");
        int port = intValue(properties, "bind.port", 7420);
        builder.bindAddress(new InetSocketAddress(host, port));

        String state = properties.getProperty("state.directory");
        if (state != null && !state.trim().isEmpty()) {
            // Against the configuration directory, not the working directory: a witness started from
            // another directory must not find an empty state directory and forget its votes.
            builder.stateDirectory(directory.resolve(state.trim()).normalize());
        }
        String source = properties.getProperty("config.source", "file").trim();
        builder.configSource(ConfigSource.valueOf(source.toUpperCase(Locale.ROOT)));
        builder.clusterApplicationLimit(intValue(properties, "config.cluster.limit", 64));
        builder.keyring(readKeyring(properties));
        warnAboutReadableSecrets(properties);
        builder.rescanInterval(rescanInterval(properties));
        builder.defaultTimings(timings(properties, Timings.lan()));
        builder.voteLogging(trueUnlessFalse(properties, "vote.logging"));
        return builder.build();
    }

    private ApplicationConfig readApplication(Path file, WitnessSettings settings) {
        String name = applicationNameOf(file);
        Properties properties = load(file, false);

        refuseKeysHere(name, properties);
        // Per application, falling back to the witness-wide value. A tenant that says nothing gets
        // what the witness was told; a tenant on another network says so in its own file, without
        // touching anyone else's.
        Timings timings = timings(properties, settings.defaultTimings());
        warnIfTimingsIgnored(name, properties, settings);
        return new ApplicationConfig(name, timings, false);
    }

    private void warnIfTimingsIgnored(String name, Properties properties, WitnessSettings settings) {
        if (!settings.configSource().isCluster()) {
            return;
        }
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("timings.") && warnedIgnoredTimings.add(name)) {
                log.warn("application '{}' sets {} but config.source=cluster, so its intervals come"
                                + " from its own nodes. The value here is read and ignored; set"
                                + " config.source=file to have this file decide instead.",
                        name, key);
                return;
            }
        }
    }

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

    private void warnIfWorldReadable(Path file) {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
            if (permissions.contains(PosixFilePermission.OTHERS_READ)) {
                log.warn("{} is world-readable and holds a cluster secret. Anyone who can"
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
            if (snapshot.settings().configSource().isCluster()) {
                log.info("no applications configured in {}; with config.source=cluster they arrive"
                        + " from the clusters themselves", applicationsDirectory);
            } else {
                log.warn("no applications configured in {}. With config.source=file this witness"
                        + " will serve nobody.", applicationsDirectory);
            }
        } else {
            log.info("serving {} application(s): {}", snapshot.applications().size(),
                    String.join(", ", snapshot.applicationNames()));
        }
        log.info("configuration source is {}", snapshot.settings().configSource());
    }
}
