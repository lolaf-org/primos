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
package org.lolaf.primos.core;

import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.store.StateStoreException;

/**
 * A store that works until it is told to break, standing in for a disk that has gone away.
 */
final class FailingStateStore implements StateStore {

    private PersistentState state = PersistentState.fresh();
    private RuntimeException failure;

    static StateStoreException brokenDisk() {
        return new StateStoreException("the disk went away");
    }

    /**
     * Every subsequent save throws this.
     */
    void breakWith(RuntimeException failure) {
        this.failure = failure;
    }

    @Override
    public PersistentState load() {
        return state;
    }

    @Override
    public void save(long currentTerm, int votedFor) {
        if (failure != null) {
            throw failure;
        }
        state = new PersistentState(currentTerm, votedFor);
    }

    @Override
    public boolean isDurable() {
        return true;
    }

    @Override
    public void close() {
        // nothing to do
    }
}
