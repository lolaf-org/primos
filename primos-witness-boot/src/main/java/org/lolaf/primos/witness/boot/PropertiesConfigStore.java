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
 * seam because restarting a witness is the one genuinely risky operation in its lifecycle (while
 * it is down, every two-node cluster it serves tolerates zero failures) so onboarding a tenant
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
     * server computes {@code deadline - now} and {@code System.nanoTime()} may be negative, where
     * the maximum would overflow into a negative wait and turn the select loop into a spin.
     */
    private static final long NEVER_NANOS = Duration.ofDays(1).toNanos();

    private final ConfigSnapshot snapshot;

    public PropertiesConfigStore(WitnessProperties properties) {
        this.snapshot = new ConfigSnapshot(settings(properties), applications(properties));
        log.warn("this witness is configured from Spring properties, so its served set and its keys"
                        + " are fixed until the process restarts: onboarding a cluster or rotating a"
                        + " secret now needs one. That is the risky operation in a witness's"
                        + " lifecycle, while it is down, every two-node cluster it serves tolerates"
                        + " zero failures. Serving {} application(s): {}",
                snapshot.applicationNames().size(), snapshot.applicationNames());
    }

    private static WitnessSettings settings(WitnessProperties properties) {
        if (properties.stateDirectory() == null) {
            throw new IllegalStateException("primos.witness.state-directory is required with"
                    + " config-store=properties: a witness must be able to remember its term and"
                    + " vote across a restart, or it can grant a second vote in a term it has"
                    + " already voted in and elect two leaders: the exact failure it was deployed"
                    + " to prevent.");
        }
        WitnessSettings.WitnessSettingsBuilder settings = WitnessSettings.builder()
                .stateDirectory(properties.stateDirectory())
                .configSource(configSource(properties))
                .clusterApplicationLimit(properties.clusterApplicationLimit())
                .defaultTimings(timings(properties.timings(), "primos.witness.timings", Timings.lan()))
                .voteLogging(properties.voteLogging())
                .keyring(keyring(properties));
        if (properties.bind().isSet()) {
            settings.bindAddress(bindAddress(properties.bind()));
        }
        return settings.build();
    }

    private static InetSocketAddress bindAddress(WitnessProperties.BindProperties bind) {
        // No file underneath to borrow from, so a port on its own means the wildcard host.
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

    private static Timings timings(WitnessProperties.TimingsProperties timings, String prefix,
                                   Timings fallback) {
        if (timings == null) {
            return fallback;
        }
        return Timings.resolve(prefix + ".profile", timings.profile() == null ? null : timings.profile().name(),
                timings.explicitKeys(prefix), fallback,
                () -> Timings.builder()
                        .heartbeatIntervalIfSet(timings.heartbeatInterval())
                        .electionTimeoutIfSet(timings.electionTimeoutMin(), timings.electionTimeoutMax(),
                                prefix + ".election-timeout-min", prefix + ".election-timeout-max")
                        .probeIntervalIfSet(timings.probeInterval())
                        .advertisementIntervalIfSet(timings.advertisementInterval())
                        .failureThresholdIfSet(timings.failureThreshold())
                        .build());
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
                    + " three-voter cluster, so this is a route to a false leader, not merely a"
                    + " denial of service.");
        }
        return new Keyring(keys);
    }

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
        // nothing to do
    }
}
