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
package org.lolaf.primos.store;

/**
 * A durable store could not do what was asked of it.
 *
 * <p>Unchecked, and deliberately fatal rather than retryable. If the term and vote cannot be made
 * durable then the node must not proceed: continuing would mean voting on state it may forget,
 * which is precisely the failure the store exists to prevent. The node thread's catch-all turns
 * this into the terminal {@code FAILED} role, so a node with a broken disk stops claiming
 * leadership instead of quietly becoming unsafe.
 */
public class StateStoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StateStoreException(String message) {
        super(message);
    }

    public StateStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
