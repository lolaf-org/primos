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
package org.lolaf.primos;

import lombok.Value;

import java.util.Objects;

/**
 * A role together with the term it was held in, read as one consistent pair.
 *
 * <p>The term is the fencing token. Guarding an external side effect on the role alone is unsafe,
 * because leadership can be lost between the check and the effect; guarding it on the term is not,
 * provided the system being guarded rejects anything carrying a term lower than the highest it has
 * seen. That is why the two are never exposed separately.
 */
@Value
public class RoleAndTerm {

    Role role;
    long term;

    public RoleAndTerm(Role role, long term) {
        this.role = Objects.requireNonNull(role, "role");
        if (term < 0) {
            throw new IllegalArgumentException("term must not be negative, but was " + term);
        }
        this.term = term;
    }

    /**
     * Whether the role is {@link Role#LEADER}.
     */
    public boolean isLeader() {
        return role.isLeader();
    }
}
