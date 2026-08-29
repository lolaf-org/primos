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

import java.nio.file.Path;

import org.lolaf.primos.store.FileStateStore;
import org.lolaf.primos.store.StateStore;

/**
 * Where one served application's term and vote are kept.
 *
 * <p>A seam with one shipped implementation, and it exists for the check rather than for the
 * choice: whatever it returns, {@link WitnessServer} refuses a store that reports itself
 * non-durable. A witness that forgets a vote and restarts can vote twice in one term and elect two
 * leaders — which is exactly the failure it was deployed to prevent — so a stateless witness is
 * not a deployment shortcut, it is the bug.
 */
@FunctionalInterface
public interface StateStoreFactory {

    /**
     * A file store under {@code <stateDirectory>/<application>/}, which is what the operator gets.
     *
     * <p>One directory per application rather than one file with a bucket per name, so that a
     * cluster's state can be moved, backed up or removed on its own, and so that two applications
     * cannot share a file lock and thereby a failure.
     */
    static StateStoreFactory perApplicationDirectory(Path stateDirectory) {
        return application -> new FileStateStore(stateDirectory.resolve(application));
    }

    /**
     * Opens the store for one application, taking whatever exclusive lock it needs.
     *
     * @param application the application name, already checked to be usable as a single path
     *                    segment
     * @throws org.lolaf.primos.store.StateStoreException if it cannot be opened
     */
    StateStore open(String application);
}
