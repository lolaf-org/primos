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
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.wire.MessageAuthenticator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One entry in a witness's keyring: a secret, its algorithm, and which applications it covers.
 *
 * <p>Keys live here rather than in an application's own file, and that is what lets them work the
 * same under both values of {@link ConfigSource}: a cluster that arrives over the network has no
 * file to have had a secret set in, so a witness that kept keys per application could only ever
 * offer such a cluster one shared key.
 */
@Getter
@EqualsAndHashCode
public final class WitnessKey {

    /**
     * What this key is called, from {@code secret.<name>.value}, so a configuration error can say which key it means.
     */
    private final String name;

    /**
     * Read through {@link #secret()}, which copies: an array handed out twice is one a caller can
     * change under this witness.
     */
    @Getter(AccessLevel.NONE)
    private final byte[] secret;

    private final String algorithm;

    /**
     * The application-name patterns this key covers, or empty when it covers anything unclaimed.
     *
     * <p>{@code *} is the only metacharacter and matches any run of characters, so {@code orders-*}
     * covers a family of clusters without naming its members, which is what makes scoping usable
     * at all under {@link ConfigSource#CLUSTER}, where the members are not known in advance.
     *
     * <p>Deliberately <strong>not</strong> a regular expression. As one, {@code orders-*} means
     * "orders followed by zero or more hyphens" and matches {@code orders--} but not
     * {@code orders-prod}: a difference nobody reads in a properties file, failing silently in
     * both directions: too narrow and the tenant cannot register, too broad and a key covers
     * clusters nobody meant it to.
     */
    private final List<String> applications;

    /**
     * @throws IllegalArgumentException if the secret is empty or the platform cannot use the algorithm, so a bad key
     *                                  fails the configuration read rather than the server loop on its first use
     */
    public WitnessKey(String name, byte[] secret, String algorithm, List<String> applications) {
        this.name = Objects.requireNonNull(name, "name");
        this.secret = Objects.requireNonNull(secret, "secret").clone();
        this.algorithm = algorithm == null || algorithm.trim().isEmpty()
                ? MessageAuthenticator.DEFAULT_ALGORITHM : algorithm.trim();
        this.applications = claimedPatterns(name, applications);
        requireUsable();
    }

    private static List<String> claimedPatterns(String keyName, List<String> configured) {
        if (configured == null) {
            return Collections.emptyList();
        }
        List<String> patterns = new ArrayList<>(configured.size());
        for (String entry : configured) {
            String pattern = entry.trim();
            if ("*".equals(pattern)) {
                // A claimed name is verified only by its claimants, so claiming everything would
                // silently take every unscoped key out of use. Same intent as claiming nothing.
                return Collections.emptyList();
            }
            if (!pattern.isEmpty()) {
                requireMatchable(keyName, pattern);
                patterns.add(pattern);
            }
        }
        return Collections.unmodifiableList(patterns);
    }

    private static void requireMatchable(String keyName, String pattern) {
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
    }

    private void requireUsable() {
        try {
            MessageAuthenticator.keyring(Collections.singletonList(
                    MessageAuthenticator.KeySpec.of(algorithm, secret)));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("key '" + name + "' cannot be used: " + e.getMessage(), e);
        }
    }

    public byte[] secret() {
        return secret.clone();
    }

    /**
     * Whether this key claims particular names rather than covering whatever is unclaimed.
     */
    public boolean isScoped() {
        return !applications.isEmpty();
    }

    /**
     * Whether this key covers that application name.
     */
    public boolean covers(String application) {
        for (String pattern : applications) {
            if (ApplicationNames.matchesPattern(pattern, application)) {
                return true;
            }
        }
        return false;
    }
}
