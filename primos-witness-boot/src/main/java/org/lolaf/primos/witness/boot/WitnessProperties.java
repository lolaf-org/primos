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

import org.lolaf.primos.Peer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Everything under {@code primos.witness.*}, which is three settings under the default store and a
 * whole configuration under the other one.
 *
 * <p><strong>Under {@code config-store=file}, the default, Spring binds three settings and no
 * more:</strong> the configuration directory, the node id and the bind address. These are the
 * tarball's three command-line options, under the names {@code witness.properties} already uses, so
 * an operator moving between packagings is not learning a second vocabulary. The state directory,
 * {@code config.source}, the keyring, the timings, vote logging and the rescan interval all keep
 * coming from {@code witness.properties} through {@link org.lolaf.primos.witness.FileConfigStore},
 * and that is a rule rather than an omission.
 *
 * <p>Spring binds once, at boot. A key expressed in both places would be silently frozen at the
 * value Spring read while an operator edited the file and watched nothing happen — and the file is
 * re-read on purpose, because <em>restarting a witness is the one genuinely risky operation in its
 * lifecycle</em>: while it is down, every two-node cluster it serves tolerates zero failures. The
 * keyring settles it on its own: a secret bound at boot cannot be rotated without the restart this
 * whole design exists to avoid.
 *
 * <p><strong>Under {@code config-store=properties} there is no file at all</strong>, so the rule
 * has nothing to protect and the rest of the settings below become live. Nothing is ever read from
 * two places; which set applies is decided by one setting, and setting the wrong half is refused at
 * startup rather than ignored.
 *
 * <p>A record with constructor binding rather than a bean with setters, for the reason
 * {@code PrimosProperties} gives in the other Spring module: this project configures Lombok with
 * fluent accessors, and Spring's JavaBean binder would not find {@code nodeId()}.
 *
 * @param configStore              where the served set comes from — see {@link Store}
 * @param configDirectory          where {@code witness.properties} and {@code applications/} live,
 *                                 under {@link Store#FILE}. The tarball's {@code --config}, and the
 *                                 same default.
 * @param nodeId                   this witness's node id. The tarball's {@code --node-id}. Durable
 *                                 in every served application's recorded vote, so treat it as fixed
 *                                 once deployed.
 * @param bind                     overrides {@code bind.host} and {@code bind.port}, the way the
 *                                 tarball's {@code --bind} does. Read under both stores.
 * @param stateDirectory           {@link Store#PROPERTIES} only. Where each application's term and
 *                                 vote are kept. Required there, and deliberately without a
 *                                 default: a default under a temp directory is silently unsafe
 *                                 after a reboot, and a witness that forgets a vote elects two
 *                                 leaders.
 * @param configSource             {@link Store#PROPERTIES} only. {@code file} serves only the
 *                                 applications listed below; {@code cluster} lets a cluster create
 *                                 itself by stating its configuration.
 * @param clusterApplicationLimit  {@link Store#PROPERTIES} only. How many applications
 *                                 {@code cluster} may create.
 * @param timings                  {@link Store#PROPERTIES} only. The intervals every served
 *                                 application runs, unless it names its own — see
 *                                 {@link TimingsProperties}.
 * @param voteLogging              {@link Store#PROPERTIES} only. Who voted for whom, at INFO.
 * @param applications             {@link Store#PROPERTIES} only. What is served, and the intervals
 *                                 each one runs — see {@link ApplicationProperties}.
 * @param keys                     {@link Store#PROPERTIES} only. The keyring — see {@link KeyProperties}
 */
@ConfigurationProperties("primos.witness")
public record WitnessProperties(
        @DefaultValue("file") Store configStore,
        @DefaultValue(WitnessProperties.DEFAULT_CONFIG_DIRECTORY) Path configDirectory,
        @DefaultValue("" + Peer.MAX_ID) int nodeId,
        @DefaultValue BindProperties bind,
        Path stateDirectory,
        @DefaultValue("file") String configSource,
        @DefaultValue("64") int clusterApplicationLimit,
        @DefaultValue TimingsProperties timings,
        @DefaultValue("true") boolean voteLogging,
        @DefaultValue List<ApplicationProperties> applications,
        @DefaultValue Map<String, KeyProperties> keys) {

    /**
     * The tarball's default, repeated here so both packagings look in the same place when nobody
     * says otherwise.
     */
    static final String DEFAULT_CONFIG_DIRECTORY = "/etc/primos-witness";

    /**
     * Where the served set and the keys come from.
     */
    public enum Store {

        /**
         * A configuration directory, re-read while the server runs. The default, and the only one
         * where onboarding a cluster does not need a restart.
         */
        FILE,

        /**
         * Spring properties, bound once at boot. For a single-tenant deployment whose served set
         * genuinely never changes — and it costs hot reload, which is not a small thing: adding a
         * cluster then means restarting the witness, and while it is down every two-node cluster it
         * serves tolerates zero failures.
         */
        PROPERTIES
    }

    /**
     * Where to listen, when the environment rather than the file is saying.
     *
     * <p>Split in two, as {@code bind.host} and {@code bind.port} are in the file, so that a
     * container can set the port alone — which is the common case — without having to restate the
     * host. Either may be given by itself; whichever is missing comes from the file.
     *
     * @param host the local address, or the wildcard when neither this nor the file names one
     * @param port the local port
     */
    public record BindProperties(String host, Integer port) {

        boolean isSet() {
            return host != null || port != null;
        }
    }

    /**
     * One served application, under {@link Store#PROPERTIES}.
     *
     * <p>A list of objects rather than a list of names, because <strong>intervals belong per
     * application</strong>. A witness serves clusters it does not own, and one of them sitting on
     * another network must not decide the intervals for all of them — which is exactly why the file
     * store keeps a file per application rather than one timings block in
     * {@code witness.properties}. A single-tenant deployment simply never fills this in and takes
     * the witness-wide default.
     *
     * @param name    the cluster's application name
     * @param timings its intervals, or null to run {@code primos.witness.timings}
     */
    public record ApplicationProperties(String name, TimingsProperties timings) {
    }

    /**
     * A profile <strong>or</strong> the intervals spelled out, never both.
     *
     * <p>A profile is a set of values chosen so the relationships between them hold — the election
     * timeout an order of magnitude above the round trip, and so on. Overriding part of one leaves a
     * custom set whose author has not seen the other values, so the mixture is refused rather than
     * merged.
     *
     * <p>Durations rather than the file's {@code .millis} integers, and that is not a second
     * vocabulary for the sake of it: under this store there is no {@code witness.properties} to be
     * consistent with, Spring parses {@code 250ms} and {@code 2s} for free, and
     * {@code primos-spring-boot-starter} — the other place a primos cluster is configured from
     * Spring — already spells them exactly this way.
     *
     * <p><strong>Whatever is set must match the clusters this witness serves.</strong> A node
     * refuses a pre-vote while it has heard from a leader within {@code electionTimeoutMin}, so a
     * witness with a longer one is proportionally more reluctant to agree to the failover its vote
     * is needed for — and for a two-node pair the witness's vote is the one everything waits on.
     * Nothing detects the mismatch; failover just gets slower.
     *
     * @param profile              {@code lan} or {@code wan}
     * @param heartbeatInterval    how often a leader speaks
     * @param electionTimeoutMin   the low end of the election timeout; a pair with the high end
     * @param electionTimeoutMax   the high end
     * @param probeInterval        how often peers are probed
     * @param advertisementInterval how often intervals are restated; never shorter than the probe
     * @param failureThreshold     missed probes before a peer is called down
     */
    public record TimingsProperties(
            Profile profile,
            Duration heartbeatInterval,
            Duration electionTimeoutMin,
            Duration electionTimeoutMax,
            Duration probeInterval,
            Duration advertisementInterval,
            Integer failureThreshold) {

        /**
         * The keys that were actually set, spelled as an operator wrote them.
         *
         * <p>Names rather than a yes-or-no, because refusing a profile beside them should quote
         * them back: the values may have come from YAML, an environment variable or the command
         * line, and "you also set some intervals" leaves the operator to search all three.
         */
        List<String> explicitKeys() {
            List<String> keys = new ArrayList<>(6);
            addIfSet(keys, "heartbeat-interval", heartbeatInterval);
            addIfSet(keys, "election-timeout-min", electionTimeoutMin);
            addIfSet(keys, "election-timeout-max", electionTimeoutMax);
            addIfSet(keys, "probe-interval", probeInterval);
            addIfSet(keys, "advertisement-interval", advertisementInterval);
            addIfSet(keys, "failure-threshold", failureThreshold);
            return keys;
        }

        private static void addIfSet(List<String> keys, String key, Object value) {
            if (value != null) {
                keys.add(key);
            }
        }
    }

    /**
     * The named interval sets. {@code wan} is for a witness on a third network, where a 50ms round
     * trip makes the LAN election timeout marginal.
     */
    public enum Profile {
        LAN,
        WAN
    }

    /**
     * One key, under {@link Store#PROPERTIES}.
     *
     * <p>The value is the secret itself, as UTF-8 bytes. The file store's {@code env:NAME},
     * {@code file:/path} and {@code base64:} forms are deliberately <strong>not</strong> repeated
     * here: Spring already resolves an environment variable into a property, so supporting a second
     * spelling of the same idea would be two mechanisms for one job, in the packaging whose whole
     * argument is configuration from the environment.
     *
     * @param name         what this key is called; a claim in the log rather than a secret
     * @param value        the shared secret
     * @param algorithm    the JCE {@code Mac}, or null for HMAC-SHA256
     * @param applications the name patterns this key may speak for, {@code *} matching any run of
     *                     characters. Empty means it covers whatever no other key claims — and a
     *                     name that any key claims is verified <em>only</em> by the keys claiming
     *                     it.
     */
    public record KeyProperties(String name, String value, String algorithm,
                                @DefaultValue List<String> applications) {
    }
}
