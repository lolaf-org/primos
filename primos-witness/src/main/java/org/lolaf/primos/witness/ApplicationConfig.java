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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.lolaf.primos.wire.MessageAuthenticator;

import java.util.Objects;

/**
 * One clustered application this witness serves.
 *
 * <p>The name is the routing key: it selects which durable term and vote a datagram belongs to.
 * That makes a name collision between two genuinely different clusters a split brain rather than
 * an inconvenience, which is why names are validated and why the strict mode exists.
 */
@Getter
@EqualsAndHashCode
public final class ApplicationConfig {

    private final String name;

    /**
     * Read through {@link #secret()} and {@link #previousSecret()}, which copy: an array handed
     * out twice is an array a caller can change under this witness.
     */
    @Getter(AccessLevel.NONE)
    private final byte[] secret;

    @Getter(AccessLevel.NONE)
    private final byte[] previousSecret;

    /**
     * The {@link javax.crypto.Mac} algorithm this application's datagrams are tagged with.
     *
     * <p>Per application rather than per witness, because a witness serves clusters it does not
     * own: one of them moving to a different algorithm must not be a flag day for every other
     * tenant. Never null — an application that says nothing gets
     * {@link MessageAuthenticator#DEFAULT_ALGORITHM}, which is what the library defaults to, so
     * the two agree without anybody configuring anything.
     */
    private final String macAlgorithm;

    /**
     * Whether this application appeared by itself rather than being configured. Read through
     * {@link #isAutoRegistered()}.
     */
    @Getter(AccessLevel.NONE)
    private final boolean autoRegistered;

    public ApplicationConfig(String name, byte[] secret, byte[] previousSecret,
                             String macAlgorithm, boolean autoRegistered) {
        this.name = Objects.requireNonNull(name, "name");
        this.secret = secret == null ? null : secret.clone();
        this.previousSecret = previousSecret == null ? null : previousSecret.clone();
        this.macAlgorithm = macAlgorithm == null
                ? MessageAuthenticator.DEFAULT_ALGORITHM
                : macAlgorithm;
        this.autoRegistered = autoRegistered;
    }

    /**
     * The secret in force, or null when this application is served without authentication.
     */
    public byte[] secret() {
        return secret == null ? null : secret.clone();
    }

    /**
     * The secret being rotated out, still accepted.
     *
     * <p>Rotation is two file edits with a window in between rather than a flag day: set the new
     * secret as current and the old one as previous, wait for every node to be updated, then
     * remove the previous one.
     */
    public byte[] previousSecret() {
        return previousSecret == null ? null : previousSecret.clone();
    }

    public boolean hasSecret() {
        return secret != null;
    }

    /**
     * Whether this application appeared by itself rather than being configured.
     */
    public boolean isAutoRegistered() {
        return autoRegistered;
    }

    /**
     * Never includes the secret.
     */
    @Override
    public String toString() {
        return "ApplicationConfig(" + name + ", secret=" + (secret != null ? "set" : "none")
                + ", previousSecret=" + (previousSecret != null ? "set" : "none")
                + ", " + macAlgorithm
                + (autoRegistered ? ", auto-registered" : "") + ")";
    }
}
