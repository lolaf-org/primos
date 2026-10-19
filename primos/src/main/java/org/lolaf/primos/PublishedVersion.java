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
package org.lolaf.primos;

import lombok.Value;

/**
 * One publication by a leader, as {@link PrimosNode#publishChange()} made it.
 *
 * <p>Store both halves with the data being published. The version restarts at 1 with every term,
 * because the counter is not replicated and a new leader cannot know where the last one stopped, so
 * a version alone orders nothing across a leader change: compare the term first.
 */
@Value
public class PublishedVersion {

    long term;
    long version;
}
