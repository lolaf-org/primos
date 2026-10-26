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

import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.Role;
import org.lolaf.primos.RoleAndTerm;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Reports this node's role, term and leader.
 *
 * <p><strong>A healthy follower is {@code UP}.</strong> Only {@link Role#FAILED} is {@code DOWN}.
 * Getting that backwards takes every follower out of a load balancer during an ordinary election —
 * a whole-cluster outage caused by the health check rather than detected by it, and the single most
 * expensive mistake this class could make.
 *
 * <p>A node that has not been started reports {@code UP} with a state of its own. It is not a
 * failure — under {@code primos.lifecycle=manual} it is the expected state until the application is
 * ready — but it must not read as an ordinary follower, because a deployment that forgot to start
 * its node looks identical to a healthy one from every other angle.
 */
public class PrimosHealthIndicator implements HealthIndicator {

    private final PrimosNode node;

    PrimosHealthIndicator(PrimosNode node) {
        this.node = node;
    }

    @Override
    public Health health() {
        RoleAndTerm current = node.roleAndTerm();
        Health.Builder health = current.role() == Role.FAILED ? Health.down() : Health.up();
        health.withDetail("application", node.config().applicationName())
                .withDetail("nodeId", node.config().nodeId())
                .withDetail("state", node.isRunning() ? "running" : "not-started")
                .withDetail("role", current.role().name())
                .withDetail("term", current.term())
                .withDetail("leader", node.leader().map(leader -> leader.isSelf()
                        ? "self" : String.valueOf(leader.id())).orElse("none"))
                .withDetail("candidate", node.isCandidate());
        if (current.role() == Role.FAILED) {
            health.withDetail("reason", "the node thread died and this node will never lead again;"
                    + " restart the process rather than waiting for it to recover");
        }
        return health.build();
    }
}
