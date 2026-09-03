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
package org.lolaf.primos.witness.boot;

import org.lolaf.primos.witness.WitnessServer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.Set;

/**
 * Reports whether this witness is serving, and which tenants it has stopped serving.
 *
 * <p><strong>One failed tenant does not make the witness {@code DOWN}.</strong> A witness serving
 * fifty clusters with one quarantined is doing its job for forty-nine, and a verdict that says
 * otherwise removes all fifty from rotation over one of them — the outage the quarantine exists to
 * avoid, arrived at from the other side. So a quarantined application belongs in the detail, where
 * somebody can act on it, and never in the status. The natural implementation aggregates over
 * tenants and gets this backwards, which is why there is a test for it.
 *
 * <p><strong>{@code DOWN} means the loop is not running</strong>, which is the one condition where
 * this process is answering HTTP and serving nobody. It is read from the thread rather than from a
 * flag that says it was asked to start: a component that reports an intention keeps a dead server
 * in a rotation.
 *
 * <p><strong>Nothing may point a liveness probe at this.</strong> Restarting a witness is the one
 * genuinely risky operation in its lifecycle — while it is down, every two-node cluster it serves
 * tolerates zero failures — so a probe that can be wrong is a mechanism for causing the outage it
 * is watching for. Readiness is fine and useful; liveness is not.
 */
public class WitnessHealthIndicator implements HealthIndicator {

    private final WitnessServer server;
    private final WitnessServerLifecycle lifecycle;

    WitnessHealthIndicator(WitnessServer server, WitnessServerLifecycle lifecycle) {
        this.server = server;
        this.lifecycle = lifecycle;
    }

    @Override
    public Health health() {
        boolean serving = lifecycle.isRunning();
        Health.Builder health = serving ? Health.up() : Health.down();
        Set<String> quarantined = server.quarantinedApplications();
        health.withDetail("boundAddress", String.valueOf(server.boundAddress()))
                .withDetail("served", server.servedApplications())
                // Always present, even when empty, so an alert can be written against a shape that
                // does not change the day something goes wrong.
                .withDetail("quarantined", quarantined);
        if (!serving) {
            health.withDetail("reason", "the witness loop is not running, so this process is"
                    + " serving nobody. Every cluster relying on it is back to the fault tolerance"
                    + " it has without one.");
        }
        if (!quarantined.isEmpty()) {
            health.withDetail("quarantineReason", "these applications' nodes failed — most often"
                    + " state that can no longer be made durable — and this witness has stopped"
                    + " answering for them. Every other application is unaffected, which is why"
                    + " this is not DOWN.");
        }
        return health.build();
    }
}
