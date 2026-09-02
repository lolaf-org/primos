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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Every key a witness holds, and the rule for which of them may speak for a given application.
 *
 * <p>The rule is one sentence: <strong>a name that any key claims is verified only by the keys
 * claiming it; a name nothing claims is verified by the keys that claim nothing.</strong>
 *
 * <p>That the claim is a reservation rather than a mere restriction is the whole of what makes
 * scoping worth having. Were a scoped key only <em>limited</em> to its names, an unscoped key
 * beside it would still verify those names too, and a tenant with its own key would gain nothing:
 * whoever held the general key could still speak for it.
 *
 * <p>The spectrum that produces, along one axis and orthogonal to {@link ConfigSource}:
 *
 * <ul>
 *   <li>every key unscoped — any cluster may arrive, and every keyholder can speak for every other,
 *       which is one team running many clusters from one template;</li>
 *   <li>some scoped — a general key for the fleet, dedicated keys for the tenants that matter;</li>
 *   <li>every key scoped — nothing but the named families can be served at all, and no keyholder
 *       can reach another's names.</li>
 * </ul>
 *
 * <p>What it is not, and the boundary is worth stating because the shape suggests otherwise: two
 * keys covering the same name — through overlapping patterns, or a pattern beside an unscoped key
 * where none claims that name — means either key verifies it. Overlap is not detected, because for
 * patterns the answer would be advisory anyway; it is documented, and the remedy is not to overlap.
 */
@Getter
@EqualsAndHashCode
public final class Keyring {

    private final List<WitnessKey> keys;

    public Keyring(List<WitnessKey> keys) {
        this.keys = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(keys, "keys")));
    }

    public static Keyring empty() {
        return new Keyring(Collections.emptyList());
    }

    public boolean isEmpty() {
        return keys.isEmpty();
    }

    /**
     * The keys that may verify datagrams for this application, in configured order.
     *
     * @return possibly empty, which means this witness cannot authenticate that name at all
     */
    public List<WitnessKey> candidatesFor(String application) {
        List<WitnessKey> claiming = new ArrayList<>(2);
        for (WitnessKey key : keys) {
            if (key.isScoped() && key.covers(application)) {
                claiming.add(key);
            }
        }
        if (!claiming.isEmpty()) {
            return claiming;
        }
        List<WitnessKey> general = new ArrayList<>(2);
        for (WitnessKey key : keys) {
            if (!key.isScoped()) {
                general.add(key);
            }
        }
        return general;
    }

    /**
     * Whether a name nobody has configured could be served, which needs a key claiming nothing.
     */
    public boolean canServeUnclaimedNames() {
        for (WitnessKey key : keys) {
            if (!key.isScoped()) {
                return true;
            }
        }
        return false;
    }

    /**
     * One line for the startup log, answering the question an operator actually has.
     *
     * <p>A keyring with nothing unscoped is a legitimate lockdown and somebody will want it — but
     * from the outside it is indistinguishable from a witness that is quietly broken, since every
     * cluster it was not told about is simply never answered. So it says which it is.
     */
    public String summary() {
        if (keys.isEmpty()) {
            return "no keys configured: this witness authenticates nothing and answers anybody";
        }
        List<String> patterns = new ArrayList<>();
        for (WitnessKey key : keys) {
            patterns.addAll(key.applications());
        }
        if (canServeUnclaimedNames()) {
            return keys.size() + " key(s)" + (patterns.isEmpty() ? "" : ", " + patterns.size()
                    + " pattern(s)") + "; applications nothing claims can still be served";
        }
        return keys.size() + " key(s), all scoped: only names matching " + String.join(", ", patterns) + " can be served";
    }
}
