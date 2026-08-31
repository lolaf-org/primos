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
 * Spring Boot auto-configuration for primos: a configured node, leadership as application events, a
 * health indicator, and an optional leadership-gated lifecycle.
 *
 * <p>Public and supported. Set {@code primos.application-name} and the rest follows; every bean
 * backs off if the application defines its own.
 *
 * <p>Two settings carry most of the meaning. {@code primos.lifecycle} decides when the node joins —
 * with the context, or when the application says so. {@code primos.leader-lifecycle} decides
 * whether leadership starts and stops beans, and which ones. Read
 * {@link org.lolaf.primos.spring.LeaderLifecycleController} before turning the second on: it makes
 * leadership convenient, not safe, and the term still has to be carried into whatever external
 * system the gated work touches.
 */
package org.lolaf.primos.spring;
