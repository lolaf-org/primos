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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.lolaf.primos.Timings;
import org.lolaf.primos.wire.MessageAuthenticator;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * The witness server's own settings, as opposed to the applications it serves.
 */
@Getter
public final class WitnessSettings {

    private final InetSocketAddress bindAddress;

    /**
     * Where per-application term and vote files live. Must be durable local storage.
     */
    private final Path stateDirectory;

    private final RegistrationMode registrationMode;

    /**
     * Read through {@link #defaultSecret()}, which copies: the array is a secret and handing the
     * same one out twice lets a caller change what this witness authenticates with.
     */
    @Getter(AccessLevel.NONE)
    private final byte[] defaultSecret;

    /**
     * The algorithm used for any application that does not name one, auto-registered ones included.
     *
     * <p>Defaults to what the library defaults to, so a witness and a cluster that have both said
     * nothing still agree. An application file may override it; nothing on the wire carries the
     * algorithm, so whatever is set here has to match what the cluster's nodes were built with.
     */
    private final String defaultMacAlgorithm;

    /**
     * How many applications may auto-register, so an open witness cannot be filled up.
     */
    private final int autoRegistrationLimit;

    /**
     * Quiet period after a file change before reloading, so one save is one reload.
     */
    private final Duration reloadDebounce;

    /**
     * How often the configuration directory is re-read regardless of events.
     *
     * <p>A safety net, not an optimisation: {@code WatchService} is inotify on Linux but a slow
     * polling fallback on macOS, and does not work at all on some network filesystems. Hot reload
     * that silently never fires is worse than none.
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

    private WitnessSettings(Builder builder) {
        this.bindAddress = Objects.requireNonNull(builder.bindAddress, "bindAddress");
        this.stateDirectory = Objects.requireNonNull(builder.stateDirectory, "stateDirectory");
        this.registrationMode = Objects.requireNonNull(builder.registrationMode, "registrationMode");
        // Copied rather than kept, because it is a secret and the caller's array is theirs.
        this.defaultSecret = builder.defaultSecret == null ? null : builder.defaultSecret.clone();
        this.defaultMacAlgorithm = Objects.requireNonNull(
                builder.defaultMacAlgorithm, "defaultMacAlgorithm");
        this.autoRegistrationLimit = builder.autoRegistrationLimit;
        this.reloadDebounce = Objects.requireNonNull(builder.reloadDebounce, "reloadDebounce");
        this.rescanInterval = Objects.requireNonNull(builder.rescanInterval, "rescanInterval");
        this.timings = Objects.requireNonNull(builder.timings, "timings");
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * The witness-wide secret, used for any application without one of its own.
     *
     * <p>It is what lets permissive mode work at all: an application nobody configured has nowhere
     * to have had a secret set. The cost is that every cluster sharing it can forge the others'
     * messages, which is fine when one team owns everything and not otherwise.
     */
    public byte[] defaultSecret() {
        return defaultSecret == null ? null : defaultSecret.clone();
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
         * Whether unconfigured applications are served. See {@link RegistrationMode}.
         */
        @Setter
        private RegistrationMode registrationMode = RegistrationMode.STRICT;

        /**
         * The witness-wide secret, used for any application without one of its own.
         */
        @Setter
        private byte[] defaultSecret;

        /**
         * The algorithm for any application that does not name one. Cluster-wide policy.
         */
        @Setter
        private String defaultMacAlgorithm = MessageAuthenticator.DEFAULT_ALGORITHM;

        /**
         * How many applications may auto-register, so an open witness cannot be filled up.
         */
        @Setter
        private int autoRegistrationLimit = 64;

        /**
         * Quiet period after a file change before reloading, so one save is one reload.
         */
        @Setter
        private Duration reloadDebounce = Duration.ofMillis(250);

        /**
         * How often the configuration directory is re-read regardless of events.
         */
        @Setter
        private Duration rescanInterval = Duration.ofSeconds(30);

        /**
         * The timing profile the served nodes run with. Keep it uniform across a cluster.
         */
        @Setter
        private Timings timings = Timings.lan();

        private Builder() {
        }

        public WitnessSettings build() {
            return new WitnessSettings(this);
        }
    }
}
