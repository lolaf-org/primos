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
     * What this key is called, from {@code secret.<name>.value}. Reaches the log, and that is most
     * of why it exists: <em>which</em> key admitted a tenant is the difference between a
     * configuration somebody can explain and one that merely works.
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
     * covers a family of clusters without naming its members — which is what makes scoping usable
     * at all under {@link ConfigSource#CLUSTER}, where the members are not known in advance.
     *
     * <p>Deliberately <strong>not</strong> a regular expression. As one, {@code orders-*} means
     * "orders followed by zero or more hyphens" and matches {@code orders--} but not
     * {@code orders-prod} — a difference nobody reads in a properties file, failing silently in
     * both directions: too narrow and the tenant cannot register, too broad and a key covers
     * clusters nobody meant it to.
     */
    private final List<String> applications;

    public WitnessKey(String name, byte[] secret, String algorithm, List<String> applications) {
        this.name = Objects.requireNonNull(name, "name");
        this.secret = Objects.requireNonNull(secret, "secret").clone();
        this.algorithm = algorithm == null || algorithm.trim().isEmpty()
                ? MessageAuthenticator.DEFAULT_ALGORITHM : algorithm.trim();
        this.applications = applications == null
                ? Collections.emptyList() : Collections.unmodifiableList(new java.util.ArrayList<>(applications));
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
