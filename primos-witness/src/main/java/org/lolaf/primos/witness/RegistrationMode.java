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

/**
 * Whether the witness serves only the applications it was told about, or any that turn up.
 */
public enum RegistrationMode {

    /**
     * Only configured applications are served; anything else is dropped and counted.
     *
     * <p>The default, because permissive mode turns a typo into a split brain. Nodes configured
     * {@code orders} and {@code ordres} drop each other's datagrams, so each sees the other as
     * unreachable; each then asks the witness, which in permissive mode creates <em>two</em>
     * buckets and grants a vote from each. Both nodes reach a majority. Two leaders, from one
     * transposed letter, with the cross-talk protection hiding the cause.
     */
    STRICT,

    /**
     * Any application name is served, creating its state on first sight.
     *
     * <p>Much less operational burden — a new cluster just points at the witness — at the cost of
     * the failure above. Deliberately available, deliberately not the default.
     */
    PERMISSIVE;

    public boolean isPermissive() {
        return this == PERMISSIVE;
    }
}
