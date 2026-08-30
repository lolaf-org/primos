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
 * Told when the served set changes.
 *
 * <p>Removing an application stops it being served; it must never delete its state, because a
 * cluster that comes back later still needs the vote it was given remembered.
 */
public interface ConfigChangeListener {

    /**
     * A new application is being served.
     */
    default void onApplicationAdded(ApplicationConfig application) {
    }

    /**
     * An application's settings changed, most often a secret being rotated.
     */
    default void onApplicationChanged(ApplicationConfig previous, ApplicationConfig current) {
    }

    /**
     * An application is no longer served. Its state stays on disk.
     */
    default void onApplicationRemoved(ApplicationConfig application) {
    }
}
