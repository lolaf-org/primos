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
 * Where the witness server's configuration comes from.
 *
 * <p>An SPI with one shipped implementation, which is unusual enough to justify. The witness is
 * long-lived shared infrastructure whose served set changes over time, and it is the one seam in
 * primos genuinely likely to gain a second implementation — etcd, or a Kubernetes ConfigMap —
 * without the server needing to change.
 *
 * <p>Reloading matters more here than it would elsewhere: restarting a witness is the one risky
 * operation in its lifecycle, because while it is down every two-node cluster it serves is back to
 * tolerating zero failures. Onboarding an application must not require it.
 *
 * <p>Implementations are driven from the server thread and need not be thread-safe, except that
 * {@link #current()} may be read from anywhere.
 */
public interface ConfigStore extends AutoCloseable {

    /**
     * The configuration in force. Never null after construction.
     */
    ConfigSnapshot current();

    /**
     * Registers a listener for changes to the served set.
     */
    void addListener(ConfigChangeListener listener);

    /**
     * Checks for changes without blocking, and publishes a new snapshot if there are any.
     *
     * <p>Folded into the server's own loop rather than given a thread: the witness runs on one
     * thread, and a watcher thread would be a second one for no gain.
     *
     * @return whether the configuration changed
     */
    boolean poll(long nowNanos);

    /**
     * When {@link #poll} should next be called, so the server can size its select timeout.
     */
    long nextCheckNanos(long nowNanos);

    @Override
    void close();
}
