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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.lolaf.primos.Timings;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * The witness server's own settings, as opposed to the applications it serves.
 *
 * <p>Value equality is not decoration: it is how a reload tells a settings-only change from no
 * change at all. {@code FileConfigStore} compared the served applications and nothing else, so a
 * changed registration mode or auto-registration limit was parsed and then dropped on the floor —
 * silently, because the snapshot it belonged to was never published.
 */
@Getter
@EqualsAndHashCode
public final class WitnessSettings {

    private final InetSocketAddress bindAddress;

    /**
     * Where per-application term and vote files live. Must be durable local storage.
     */
    private final Path stateDirectory;

    private final ConfigSource configSource;

    /**
     * Every key this witness holds, and which applications each may speak for.
     *
     * <p>Witness-wide rather than per application, and that is what makes keys independent of
     * {@link ConfigSource}: a cluster arriving over the network has no file to have had a secret
     * set in, so keys kept per application could offer it only one shared key or none.
     */
    private final Keyring keyring;

    /**
     * How many applications {@link ConfigSource#CLUSTER} may create, so an open witness cannot be
     * filled up by whoever can reach the port.
     */
    private final int clusterApplicationLimit;

    /**
     * How often the configuration is checked for change — the whole of the reload mechanism.
     *
     * <p>Not a safety net any more: it replaced the {@code WatchService} that used to sit above it,
     * because a watcher that can silently stop working is worse than a check that always runs. Each
     * tick stats the files and does nothing further unless one has moved, so this is cheap enough
     * to be seconds rather than the half-minute it was when every tick reparsed the directory.
     */
    private final Duration rescanInterval;

    /**
     * The timing profile the served nodes run with.
     *
     * <p>Defaults to {@link Timings#lan()}, matching the library, because a witness whose pre-vote
     * window differs from its cluster's answers questions the cluster is not asking. A witness
     * genuinely on a third network wants {@link Timings#wan()}: a 50ms WAN round trip makes the
     * LAN election timeout marginal.
     */
    private final Timings timings;

    /**
     * Whether the served nodes log who voted for whom at INFO rather than DEBUG.
     *
     * <p>On by default, and worth more here than anywhere else: the witness is the vote that breaks
     * the tie, so "did the witness grant it, and to whom?" is the first question asked after a
     * two-node pair failed over — or failed to. Witness-wide rather than per application, like the
     * timing profile, because it describes how this process talks about its work rather than
     * anything about a tenant's cluster.
     */
    private final boolean voteLogging;

    private WitnessSettings(Builder builder) {
        this.bindAddress = Objects.requireNonNull(builder.bindAddress, "bindAddress");
        this.stateDirectory = Objects.requireNonNull(builder.stateDirectory, "stateDirectory");
        this.configSource = Objects.requireNonNull(builder.configSource, "configSource");
        this.keyring = Objects.requireNonNull(builder.keyring, "keyring");
        this.clusterApplicationLimit = builder.clusterApplicationLimit;
        this.rescanInterval = Objects.requireNonNull(builder.rescanInterval, "rescanInterval");
        this.timings = Objects.requireNonNull(builder.timings, "timings");
        this.voteLogging = builder.voteLogging;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link WitnessSettings}.
     */
    public static final class Builder {

        /**
         * Where to listen. Not hot-reloadable: a change is read and ignored until a restart.
         */
        @Setter
        private InetSocketAddress bindAddress = new InetSocketAddress(7420);

        /**
         * Where per-application term and vote files live. Must be durable local storage.
         */
        @Setter
        private Path stateDirectory;

        /**
         * Where a served application's configuration comes from. See {@link ConfigSource}.
         */
        @Setter
        private ConfigSource configSource = ConfigSource.FILE;

        /**
         * Every key this witness holds. See {@link Keyring}.
         */
        @Setter
        private Keyring keyring = Keyring.empty();

        /**
         * How many applications {@link ConfigSource#CLUSTER} may create.
         */
        @Setter
        private int clusterApplicationLimit = 64;

        /**
         * How often the configuration directory is re-read regardless of events.
         */
        @Setter
        private Duration rescanInterval = Duration.ofSeconds(5);

        /**
         * The timing profile the served nodes run with. Keep it uniform across a cluster.
         */
        @Setter
        private Timings timings = Timings.lan();

        /**
         * Whether the served nodes log who voted for whom at INFO rather than DEBUG. On by default.
         */
        @Setter
        private boolean voteLogging = true;

        private Builder() {
        }

        public WitnessSettings build() {
            return new WitnessSettings(this);
        }
    }
}
