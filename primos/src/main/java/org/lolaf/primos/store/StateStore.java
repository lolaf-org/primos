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
 * Where a node's term and vote are kept.
 *
 * <p>The contract that matters is timing: {@link #save} must not return until the values are on
 * stable storage. A node persists before it acts — before granting a vote, before starting an
 * election, and before replying to anything that raised its term — so a store that returns early
 * silently converts a durable protocol into a lossy one.
 *
 * <p>Both fields are written in one call. There is deliberately no {@code setTerm} and no
 * {@code setVote}: they must land together or not at all.
 *
 * <p>Implementations are owned by one node thread and need not be thread-safe.
 */
public interface StateStore extends AutoCloseable {

    /**
     * Reads the state back, once, at startup.
     *
     * @return {@link PersistentState#fresh()} if this node has never run
     * @throws StateStoreException if state exists but cannot be trusted — which must never be
     *                             quietly treated as a fresh node, since that is how a vote is
     *                             forgotten and a second leader elected
     */
    PersistentState load();

    /**
     * Makes both fields durable, returning only once they are.
     *
     * @throws StateStoreException if they could not be made durable
     */
    void save(long currentTerm, int votedFor);

    /**
     * Whether this store actually survives a crash.
     *
     * <p>A cluster running on a store that does not is unsafe after any restart. A node logs a
     * warning and continues, because an application may legitimately want that for a test; the
     * witness server refuses outright, because a stateless witness is the double-vote bug and the
     * choice there belongs to the operator rather than to a caller.
     */
    boolean isDurable();

    /**
     * Releases whatever the store holds. Idempotent.
     */
    @Override
    void close();
}
