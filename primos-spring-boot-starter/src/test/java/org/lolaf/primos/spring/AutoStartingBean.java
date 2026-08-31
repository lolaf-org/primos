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

import org.springframework.context.SmartLifecycle;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A gated bean that auto-starts, which is the shape that makes the trap real: a
 * {@link SmartLifecycle} that starts itself is already running before the first election has
 * concluded.
 */
public class AutoStartingBean implements SmartLifecycle {

    private final AtomicInteger starts = new AtomicInteger();
    private final AtomicInteger stops = new AtomicInteger();
    private volatile boolean running;

    @Override
    public void start() {
        running = true;
        starts.incrementAndGet();
    }

    @Override
    public void stop() {
        running = false;
        stops.incrementAndGet();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    public int starts() {
        return starts.get();
    }

    public int stops() {
        return stops.get();
    }
}
