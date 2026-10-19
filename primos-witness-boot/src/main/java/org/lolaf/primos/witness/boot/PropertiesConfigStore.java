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

import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.Timings;
import org.lolaf.primos.witness.*;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * The whole configuration from Spring properties, bound once at boot and never re-read.
 *
 * <p><strong>Opt-in, and it costs the thing this SPI exists for.</strong> {@link ConfigStore} is a
 * seam because restarting a witness is the one genuinely risky operation in its lifecycle — while
 * it is down, every two-node cluster it serves tolerates zero failures — so onboarding a tenant
 * must not need one. This implementation trades that away: adding a cluster, or rotating a secret,
 * means a restart. It is a legitimate choice for a single-tenant deployment whose served set
 * genuinely never changes, and it is a bad one for anything else, which is why choosing it logs a
 * warning saying so rather than doing it quietly.
 *
 * <p>Everything else about the witness is unchanged. This class produces a {@link ConfigSnapshot}
 * and stops; the server, the routing, the admission checks and every invariant are the same ones
 * the file store feeds.
 */
@Slf4j
public class PropertiesConfigStore implements ConfigStore {

    /**
     * How far ahead "never" is. A finite interval rather than {@link Long#MAX_VALUE}, because the
     * server computes {@code deadline - now} and {@code System.nanoTime()} may be negative — where
     * the maximum would overflow into a negative wait and turn the select loop into a spin.
     */
    private static final long NEVER_NANOS = Duration.ofDays(1).toNanos();

    private final ConfigSnapshot snapshot;

    public PropertiesConfigStore(WitnessProperties properties) {
        this.snapshot = new ConfigSnapshot(settings(properties), applications(properties));
        log.warn("this witness is configured from Spring properties, so its served set and its keys"
                        + " are fixed until the process restarts: onboarding a cluster or rotating a"
                        + " secret now needs one. That is the risky operation in a witness's"
                        + " lifecycle — while it is down, every two-node cluster it serves tolerates"
                        + " zero failures. Serving {} application(s): {}",
                snapshot.applicationNames().size(), snapshot.applicationNames());
    }

    private static WitnessSettings settings(WitnessProperties properties) {
        if (properties.stateDirectory() == null) {
            throw new IllegalStateException("primos.witness.state-directory is required with"
                    + " config-store=properties: a witness must be able to remember its term and"
                    + " vote across a restart, or it can grant a second vote in a term it has"
                    + " already voted in and elect two leaders — the exact failure it was deployed"
                    + " to prevent.");
        }
        WitnessSettings.Builder settings = WitnessSettings.builder()
                .stateDirectory(properties.stateDirectory())
                .configSource(configSource(properties))
                .clusterApplicationLimit(properties.clusterApplicationLimit())
                .defaultTimings(timings(properties.timings(), "primos.witness.timings", Timings.lan()))
                .voteLogging(properties.voteLogging())
                .keyring(keyring(properties));
        InetSocketAddress bind = bindAddress(properties);
        if (bind != null) {
            settings.bindAddress(bind);
        }
        return settings.build();
    }

    /**
     * The bind address, or null to leave the builder's own default alone.
     *
     * <p>There is no file underneath this store, so a half-given address cannot borrow the other
     * half from one: a port on its own means the wildcard host, which is what a container wants
     * anyway.
     */
    private static InetSocketAddress bindAddress(WitnessProperties properties) {
        WitnessProperties.BindProperties bind = properties.bind();
        if (!bind.isSet()) {
            return null;
        }
        int port = bind.port() != null ? bind.port() : 7420;
        return bind.host() == null || bind.host().isEmpty()
                ? new InetSocketAddress(port)
                : new InetSocketAddress(bind.host(), port);
    }

    private static ConfigSource configSource(WitnessProperties properties) {
        String source = properties.configSource().trim().toUpperCase(Locale.ROOT);
        try {
            return ConfigSource.valueOf(source);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("primos.witness.config-source must be 'file' or"
                    + " 'cluster', but was '" + properties.configSource() + "'");
        }
    }

    /**
     * A profile, or the intervals spelled out, or whatever the level above decided — never a
     * mixture.
     *
     * <p>The same rule and the same reasoning as the file store's {@code timings.*} block: a
     * profile is a set of values picked so the relationships between them hold, so overriding part
     * of one leaves a custom set whose author has not seen the rest. The mixture is refused rather
     * than merged, and the message quotes the keys back because they may have come from YAML, an
     * environment variable or the command line.
     *
     * @param prefix   how an operator spells these keys, so an error names what they wrote
     * @param fallback what applies when nothing here is set — the witness-wide values for an
     *                 application, and the LAN profile for the witness itself
     */
    private static Timings timings(WitnessProperties.TimingsProperties timings, String prefix,
                                   Timings fallback) {
        if (timings == null) {
            return fallback;
        }
        List<String> explicit = timings.explicitKeys();
        if (timings.profile() != null && !explicit.isEmpty()) {
            throw new IllegalStateException(prefix + ".profile is set to '"
                    + timings.profile().name().toLowerCase(Locale.ROOT) + "' and so is " + prefix
                    + "." + String.join(", " + prefix + ".", explicit) + ". Choose one: a profile is"
                    + " a set of intervals picked to hold together, so overriding part of one leaves"
                    + " a custom set whose author has not seen the other values. Either drop the"
                    + " profile and name every interval you want, or drop the individual keys.");
        }
        if (timings.profile() != null) {
            return timings.profile() == WitnessProperties.Profile.WAN ? Timings.wan() : Timings.lan();
        }
        if (explicit.isEmpty()) {
            return fallback;
        }
        // The two bounds are a pair: setting one against the other's default is how an invalid
        // range is built. Timings rejects that at build(), but naming the property is a better
        // error than naming a Java field, so it is caught here first.
        boolean hasMin = timings.electionTimeoutMin() != null;
        boolean hasMax = timings.electionTimeoutMax() != null;
        if (hasMin != hasMax) {
            throw new IllegalStateException(prefix + ".election-timeout-" + (hasMin ? "min" : "max")
                    + " is set without its pair. The election timeout is a range drawn from afresh"
                    + " on every reset, and setting one end against the other's default is how an"
                    + " invalid or too-narrow range is built. Set both.");
        }
        Timings.TimingsBuilder builder = Timings.builder();
        if (timings.heartbeatInterval() != null) {
            builder.heartbeatInterval(timings.heartbeatInterval());
        }
        if (hasMin) {
            builder.electionTimeoutMin(timings.electionTimeoutMin());
            builder.electionTimeoutMax(timings.electionTimeoutMax());
        }
        if (timings.probeInterval() != null) {
            builder.probeInterval(timings.probeInterval());
        }
        if (timings.advertisementInterval() != null) {
            builder.advertisementInterval(timings.advertisementInterval());
        }
        if (timings.failureThreshold() != null) {
            builder.failureThreshold(timings.failureThreshold());
        }
        // Every relationship between these — the election timeout above five heartbeats, the range
        // wider than one heartbeat, the probe ceiling — is checked in here, with its own message.
        return builder.build();
    }

    private static Keyring keyring(WitnessProperties properties) {
        List<WitnessKey> keys = new ArrayList<>();
        properties.keys().forEach((key, configured) -> {
            if (configured.value() == null || configured.value().isEmpty()) {
                throw new IllegalStateException("primos.witness.keys." + key + ".value is required"
                        + " once the key is named at all");
            }
            String name = configured.name() != null ? configured.name() : key;
            keys.add(new WitnessKey(name, configured.value().getBytes(StandardCharsets.UTF_8),
                    configured.algorithm(), configured.applications()));
        });
        if (keys.isEmpty()) {
            log.warn("no keys are configured. An unauthenticated witness answers anybody who asks,"
                    + " and one forged vote grant plus a candidate's own vote is a majority in a"
                    + " three-voter cluster — so this is a route to a false leader, not merely a"
                    + " denial of service.");
        }
        return new Keyring(keys);
    }

    /**
     * The served set, each application on its own intervals or on the witness's.
     *
     * <p>Per application rather than one block for the server, because a witness serves clusters it
     * does not own: one of them being on another network must not decide the intervals for all of
     * them. That is the same reason the file store keeps a file per application.
     */
    private static Map<String, ApplicationConfig> applications(WitnessProperties properties) {
        Timings witnessWide = timings(properties.timings(), "primos.witness.timings", Timings.lan());
        Map<String, ApplicationConfig> applications = new LinkedHashMap<>();
        for (WitnessProperties.ApplicationProperties application : properties.applications()) {
            if (application.name() == null || application.name().trim().isEmpty()) {
                throw new IllegalStateException("every entry in primos.witness.applications needs a"
                        + " name: it is the cluster's application name, and it is what every"
                        + " datagram is routed by.");
            }
            String name = application.name().trim();
            applications.put(name, new ApplicationConfig(name,
                    timings(application.timings(),
                            "primos.witness.applications[" + name + "].timings", witnessWide),
                    false));
        }
        return applications;
    }

    @Override
    public ConfigSnapshot current() {
        return snapshot;
    }

    /**
     * Accepted and never called, because nothing here ever changes. Refusing a listener would make
     * this store a different SPI rather than a different implementation of the same one.
     */
    @Override
    public void addListener(ConfigChangeListener listener) {
    }

    @Override
    public boolean poll(long nowNanos) {
        return false;
    }

    @Override
    public long nextCheckNanos(long nowNanos) {
        return nowNanos + NEVER_NANOS;
    }

    @Override
    public void close() {
    }
}
