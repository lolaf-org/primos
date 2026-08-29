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
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

import org.lolaf.primos.Timings;

/** The witness server's own settings, as opposed to the applications it serves. */
public final class WitnessSettings {

    private final InetSocketAddress bindAddress;
    private final Path stateDirectory;
    private final RegistrationMode registrationMode;
    private final byte[] defaultSecret;
    private final int autoRegistrationLimit;
    private final Duration reloadDebounce;
    private final Duration rescanInterval;
    private final Timings timings;

    private WitnessSettings(Builder builder) {
        this.bindAddress = Objects.requireNonNull(builder.bindAddress, "bindAddress");
        this.stateDirectory = Objects.requireNonNull(builder.stateDirectory, "stateDirectory");
        this.registrationMode = builder.registrationMode;
        this.defaultSecret = builder.defaultSecret == null ? null : builder.defaultSecret.clone();
        this.autoRegistrationLimit = builder.autoRegistrationLimit;
        this.reloadDebounce = builder.reloadDebounce;
        this.rescanInterval = builder.rescanInterval;
        this.timings = Objects.requireNonNull(builder.timings, "timings");
    }

    public static Builder builder() {
        return new Builder();
    }

    public InetSocketAddress bindAddress() {
        return bindAddress;
    }

    /** Where per-application term and vote files live. Must be durable local storage. */
    public Path stateDirectory() {
        return stateDirectory;
    }

    public RegistrationMode registrationMode() {
        return registrationMode;
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

    /** How many applications may auto-register, so an open witness cannot be filled up. */
    public int autoRegistrationLimit() {
        return autoRegistrationLimit;
    }

    /** Quiet period after a file change before reloading, so one save is one reload. */
    public Duration reloadDebounce() {
        return reloadDebounce;
    }

    /**
     * How often the configuration directory is re-read regardless of events.
     *
     * <p>A safety net, not an optimisation: {@code WatchService} is inotify on Linux but a slow
     * polling fallback on macOS, and does not work at all on some network filesystems. Hot reload
     * that silently never fires is worse than none.
     */
    public Duration rescanInterval() {
        return rescanInterval;
    }

    /**
     * The timing profile the served nodes run with.
     *
     * <p>Defaults to {@link Timings#lan()}, matching the library, because a witness whose pre-vote
     * window differs from its cluster's answers questions the cluster is not asking. A witness
     * genuinely on a third network wants {@link Timings#wan()}: a 50ms WAN round trip makes the
     * LAN election timeout marginal.
     */
    public Timings timings() {
        return timings;
    }

    public static final class Builder {

        private InetSocketAddress bindAddress = new InetSocketAddress(7420);
        private Path stateDirectory;
        private RegistrationMode registrationMode = RegistrationMode.STRICT;
        private byte[] defaultSecret;
        private int autoRegistrationLimit = 64;
        private Duration reloadDebounce = Duration.ofMillis(250);
        private Duration rescanInterval = Duration.ofSeconds(30);
        private Timings timings = Timings.lan();

        private Builder() {
        }

        public Builder bindAddress(InetSocketAddress bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Builder stateDirectory(Path stateDirectory) {
            this.stateDirectory = stateDirectory;
            return this;
        }

        public Builder registrationMode(RegistrationMode registrationMode) {
            this.registrationMode = Objects.requireNonNull(registrationMode, "registrationMode");
            return this;
        }

        public Builder defaultSecret(byte[] defaultSecret) {
            this.defaultSecret = defaultSecret;
            return this;
        }

        public Builder autoRegistrationLimit(int autoRegistrationLimit) {
            this.autoRegistrationLimit = autoRegistrationLimit;
            return this;
        }

        public Builder reloadDebounce(Duration reloadDebounce) {
            this.reloadDebounce = reloadDebounce;
            return this;
        }

        public Builder rescanInterval(Duration rescanInterval) {
            this.rescanInterval = rescanInterval;
            return this;
        }

        public Builder timings(Timings timings) {
            this.timings = Objects.requireNonNull(timings, "timings");
            return this;
        }

        public WitnessSettings build() {
            return new WitnessSettings(this);
        }
    }
}
