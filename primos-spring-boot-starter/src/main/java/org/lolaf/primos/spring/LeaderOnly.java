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
package org.lolaf.primos.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@link org.springframework.context.Lifecycle} bean that should run only while this node
 * is the leader.
 *
 * <p>Has an effect only when {@code primos.leader-lifecycle.enabled} is true and the scope is
 * {@code beans} with no explicit bean list set — an explicit list wins, so that an application can
 * override the annotations without editing them.
 *
 * <p><strong>It makes leadership convenient, not safe.</strong> Between losing leadership and the
 * stop completing, in-flight work is still running, and a {@code Lifecycle} stop cannot be fenced.
 * Carry the term into whatever external system the work touches and have that system reject
 * anything below the highest term it has seen. This annotation is the most likely thing in the
 * module to be mistaken for a solution to that, which is why the warning is here rather than only
 * in the README.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LeaderOnly {
}
