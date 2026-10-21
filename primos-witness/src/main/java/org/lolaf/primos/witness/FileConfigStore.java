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
import java.util.concurrent.CopyOnWriteArrayList;
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
    private static final String KEY_PREFIX = "secret.";

    private final Path directory;
    private final Path applicationsDirectory;
    private final List<ConfigChangeListener> listeners = new CopyOnWriteArrayList<>();

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
    private long stamp;
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
     * @throws IllegalArgumentException if the initial configuration cannot be read at all — a
     *                                  server that cannot be configured must not start
     */
    public FileConfigStore(Path directory, MonotonicClock clock) {
        this.directory = directory;
        this.applicationsDirectory = directory.resolve(APPLICATIONS_DIRECTORY);
        this.current = read();
        this.nextRescanNanos = clock.nanoTime() + current.settings().rescanInterval().toNanos();
        this.stamp = stamp();
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
    private static Timings timings(Properties properties, Timings fallback) {
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
            return fallback;
        }
        // The two bounds are a pair: setting one against the other's default is how an invalid
        // range is built. Timings rejects that at build(), but naming the property is a better
        // error than naming a Java field, so it is caught here first.
        if ((electionMin == null) != (electionMax == null)) {
            throw new IllegalArgumentException("timings.election."
                    + (electionMin != null ? "min" : "max") + ".millis is set without its pair. The"
                    + " election timeout is a range drawn from afresh on every reset, and setting"
                    + " one end against the other's default is how an invalid or too-narrow range"
                    + " is built. Set both.");
        }
        Timings.TimingsBuilder builder = Timings.builder();
        if (heartbeat != null) {
            builder.heartbeatInterval(Duration.ofMillis(heartbeat));
        }
        if (electionMin != null) {
            builder.electionTimeoutMin(Duration.ofMillis(electionMin));
            builder.electionTimeoutMax(Duration.ofMillis(electionMax));
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
     * The application patterns a key claims, validated as far as they can be before anything runs.
     *
     * <p>A bare {@code *} is treated as claiming nothing, which is the same thing as omitting the
     * setting. Otherwise it would be a trap: a claimed name is verified <em>only</em> by the keys
     * claiming it, so one {@code *} entry would silently take every unscoped key in the file out of
     * use.
     */
    private static List<String> patterns(String keyName, String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> patterns = new ArrayList<>();
        for (String piece : raw.split(",")) {
            String pattern = piece.trim();
            if (pattern.isEmpty()) {
                continue;
            }
            if ("*".equals(pattern)) {
                // Claims everything, which is the same intent as claiming nothing.
                return Collections.emptyList();
            }
            int literal = ApplicationNames.literalLength(pattern);
            if (literal > ClusterConfig.APPLICATION_NAME_MAX_BYTES) {
                throw new IllegalArgumentException("key '" + keyName + "' claims '" + pattern
                        + "', whose " + literal + " literal characters cannot fit the "
                        + ClusterConfig.APPLICATION_NAME_MAX_BYTES
                        + "-byte name field, so it can never match anything");
            }
            for (int i = 0; i < pattern.length(); i++) {
                char c = pattern.charAt(i);
                if (c != '*' && !ClusterConfig.isApplicationNameCharacter(c)) {
                    throw new IllegalArgumentException("key '" + keyName + "' claims '" + pattern
                            + "', which contains a character no application name may carry;"
                            + " patterns match names, and a name is printable ASCII without spaces");
                }
            }
            patterns.add(pattern);
        }
        return patterns;
    }

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

    /**
     * Says so when an application file still carries key material, which no longer lives there.
     *
     * <p>Refused rather than ignored. Every other stale key in this estate is read and dropped with
     * a line saying so, and this one is not, because the failure it would otherwise produce is the
     * worst kind available here: an operator who believes a tenant has its own secret, and a
     * witness verifying it with whatever the keyring happens to offer. A configuration that is
     * wrong about who can speak for whom must not start.
     */
    private static void warnIfKeysHere(String name, Properties properties) {
        for (String property : properties.stringPropertyNames()) {
            if (property.equals("secret") || property.startsWith(KEY_PREFIX)) {
                throw new IllegalArgumentException("application '" + name + "' sets '" + property
                        + "', but keys live in witness.properties as secret.<name>.value, with an"
                        + " optional secret.<name>.applications naming what each covers. Leaving"
                        + " this here would mean believing this tenant has its own key when the"
                        + " keyring decides otherwise.");
            }
        }
    }

    /**
     * Every {@code secret.<name>.value} in the file, with its algorithm and the names it claims.
     *
     * <p>Sorted by key name, so the ring's order does not depend on {@code Properties} hashing.
     * Order decides nothing about correctness — a datagram is authentic if any candidate accepts
     * it — but a configuration that reads differently on two runs is not one anybody can reason
     * about.
     */
    private Keyring readKeyring(Properties properties) {
        Set<String> names = new TreeSet<>();
        for (String property : properties.stringPropertyNames()) {
            if (!property.startsWith(KEY_PREFIX)) {
                continue;
            }
            String rest = property.substring(KEY_PREFIX.length());
            int dot = rest.indexOf('.');
            if (dot <= 0) {
                throw new IllegalArgumentException("'" + property + "' names no key: a secret is"
                        + " configured as secret.<name>.value, optionally with"
                        + " secret.<name>.algorithm and secret.<name>.applications");
            }
            names.add(rest.substring(0, dot));
        }
        List<WitnessKey> keys = new ArrayList<>(names.size());
        for (String name : names) {
            byte[] secret = secretValue(properties, KEY_PREFIX + name + ".value", directory);
            if (secret == null) {
                throw new IllegalArgumentException("key '" + name + "' has no "
                        + KEY_PREFIX + name + ".value; a key without a secret cannot verify"
                        + " anything and is more likely a typo than an intention");
            }
            keys.add(new WitnessKey(name, secret,
                    properties.getProperty(KEY_PREFIX + name + ".algorithm"),
                    patterns(name, properties.getProperty(KEY_PREFIX + name + ".applications"))));
        }
        return new Keyring(keys);
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
        if (closed || nowNanos < nextRescanNanos) {
            return false;
        }
        nextRescanNanos = nowNanos + current.settings().rescanInterval().toNanos();

        long seen = stamp();
        if (seen == stamp) {
            // The common case by an enormous margin, and it costs a handful of stats rather than
            // parsing every file to discover nothing moved.
            return false;
        }
        stamp = seen;
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
        // Both halves, because the settings are half the file. Comparing only the applications
        // left a settings-only edit parsed and then discarded: the snapshot was never swapped, so
        // config.source, config.cluster.limit and the two reload intervals were read from disk
        // every rescan and never took effect.
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
            announce(previous, candidate);
        }
        return true;
    }

    /**
     * Says what was read and not applied, rather than half-applying it.
     *
     * <p>Everything else in {@code witness.properties} takes effect on the next reload. These three
     * cannot: the socket is bound once, the state directory is opened once, and vote logging is
     * taken by the server when it starts. Publishing the new snapshot without saying so would leave
     * the file on disk disagreeing with the running process, with nothing anywhere to say which one
     * was winning — and the witness manual has promised this line since it was written.
     */
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
     * A checksum over every file the configuration is made of, names included.
     *
     * <p>This replaced a {@code WatchService} on 2026-09-02, and the reason is that the watcher was
     * never the mechanism — the periodic re-read below it always ran regardless, so the watcher only
     * ever bought latency. It bought it in the one currency this file will not spend: it could stop
     * working without saying so. On a Kubernetes ConfigMap it does exactly that. The volume is a
     * symlink farm; an update writes a new timestamped directory, swaps {@code ..data} atomically
     * and deletes the old one — so a watch registered on the resolved directory is now watching
     * something deleted, never fires again, and says nothing. On macOS the JDK has no native backend
     * and polls every few seconds anyway; on NFS it does nothing at all. Now bare metal, a network
     * filesystem, macOS and a ConfigMap are one code path, with no environment in which the
     * mechanism quietly does less than it claims.
     *
     * <p><strong>Content rather than size and modification time</strong>, which is what this was
     * first written as. A test caught it immediately: changing {@code bind.port} from 9999 to 8888
     * is the same length, and filesystem timestamps are coarse enough that two writes can share
     * one — so an entirely ordinary edit was invisible. Reading the files is affordable precisely
     * because this replaced something more expensive: the old design parsed every file every thirty
     * seconds, and this reads a few kilobytes every five and parses only when something moved.
     *
     * <p>{@link CRC32} for the same reason {@code FileStateStore} uses it — this detects an edit,
     * it does not defend against one. The whole directory is re-listed each time rather than only
     * the files already known, because a file being dropped in is the case this exists for, and
     * names are folded into the sum so that a rename with identical content still counts.
     */
    private long stamp() {
        CRC32 sum = new CRC32();
        absorb(sum, directory.resolve(SETTINGS_FILE));
        if (Files.isDirectory(applicationsDirectory)) {
            List<Path> files = new ArrayList<>();
            try (DirectoryStream<Path> stream =
                         Files.newDirectoryStream(applicationsDirectory, "*" + SUFFIX)) {
                stream.forEach(files::add);
            } catch (IOException e) {
                // Unreadable is a state like any other: it sums differently from readable, so it
                // reloads once and the existing keep-the-last-good-configuration path reports it.
                sum.update('?');
                return sum.getValue();
            }
            files.sort(Comparator.comparing(Path::toString));
            for (Path file : files) {
                absorb(sum, file);
            }
        }
        return sum.getValue();
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
            // Against the configuration directory, not the working directory: a witness started from
            // another directory must not find an empty state directory and forget its votes.
            builder.stateDirectory(directory.resolve(state.trim()).normalize());
        }
        String source = properties.getProperty("config.source", "file").trim();
        builder.configSource(ConfigSource.valueOf(source.toUpperCase(Locale.ROOT)));
        builder.clusterApplicationLimit(intValue(properties, "config.cluster.limit", 64));
        builder.keyring(readKeyring(properties));
        builder.rescanInterval(Duration.ofSeconds(intValue(properties, "reload.rescan.seconds", 5)));
        builder.defaultTimings(timings(properties, Timings.lan()));
        builder.voteLogging(booleanValue(properties, "vote.logging", true));
        return builder.build();
    }

    private ApplicationConfig readApplication(Path file, WitnessSettings settings) {
        String name = applicationNameOf(file);
        warnIfWorldReadable(file);
        Properties properties = load(file, false);

        warnIfKeysHere(name, properties);
        // Per application, falling back to the witness-wide value. A tenant that says nothing gets
        // what the witness was told; a tenant on another network says so in its own file, without
        // touching anyone else's.
        Timings timings = timings(properties, settings.defaultTimings());
        warnIfTimingsIgnored(name, properties, settings);
        return new ApplicationConfig(name, timings, false);
    }

    /**
     * Says so when a file sets intervals that {@link ConfigSource#CLUSTER} will overrule.
     *
     * <p>Under that setting a cluster states its own profile and the witness follows it, so
     * whatever is written here is read, parsed, and then replaced by the first thing its nodes
     * say. One source or the other, never a merge of the two — but a key that is quietly ignored
     * is worse than one that is refused, so it is said out loud, once per application.
     */
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

    /**
     * Says what is being served, and warns only where an empty set is actually wrong.
     *
     * <p>Under {@link ConfigSource#CLUSTER} an empty {@code applications/} is the intended shape,
     * not a mistake: applications are created from the config messages their nodes send, timings
     * come from those messages, and keys live in the keyring — so a plug-and-play witness needs
     * that directory not to exist at all. Warning there would put a line in every startup log of a
     * correct deployment, which is how a warning stops being read.
     */
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
