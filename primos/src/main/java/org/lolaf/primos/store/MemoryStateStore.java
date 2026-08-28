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
 * Keeps the term and vote in a field.
 *
 * <p>For the deterministic simulation and for tests. The simulation models a crash by discarding
 * everything not yet made durable, and this store makes that trivial — but it also means a real
 * cluster running on it is unsafe after any restart, which is why {@link #isDurable()} says so.
 */
public final class MemoryStateStore implements StateStore {

    private PersistentState state;

    /** A store for a node that has never run. */
    public MemoryStateStore() {
        this(PersistentState.fresh());
    }

    /** A store preloaded with state, for tests that begin partway through a cluster's life. */
    public MemoryStateStore(PersistentState state) {
        this.state = state;
    }

    @Override
    public PersistentState load() {
        return state;
    }

    @Override
    public void save(long currentTerm, int votedFor) {
        state = new PersistentState(currentTerm, votedFor);
    }

    @Override
    public boolean isDurable() {
        return false;
    }

    @Override
    public void close() {
        // Nothing to release.
    }
}
