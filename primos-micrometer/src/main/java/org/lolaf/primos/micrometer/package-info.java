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

/**
 * Binds primos onto a Micrometer registry: {@link org.lolaf.primos.micrometer.PrimosMetrics} for a
 * node, {@link org.lolaf.primos.micrometer.WitnessMetrics} for a witness server and every cluster
 * it serves.
 *
 * <p>Supported API. It is a separate artifact so that the library never references a Micrometer
 * type and an application that wants no Micrometer receives none.
 */
package org.lolaf.primos.micrometer;
