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
 * The witness server as a Spring Boot application.
 *
 * <p>One of two front doors onto {@code primos-witness}; the other is the tarball in
 * {@code primos-witness-cli}. This one is for people who already run containers and want actuator,
 * configuration from the environment and a metrics endpoint without wiring any of it. Neither may
 * hold logic of its own.
 */
package org.lolaf.primos.witness.boot;
