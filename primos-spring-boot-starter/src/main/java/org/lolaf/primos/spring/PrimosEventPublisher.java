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

import org.lolaf.primos.Leader;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.Role;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Optional;

/**
 * Republishes primos' callbacks as Spring application events.
 *
 * <p>Runs wherever the node's listener dispatch runs. The auto-configuration gives the node an
 * executor for that, so publishing — and therefore every {@code @EventListener} downstream of it —
 * is off the node thread, where a slow listener would otherwise stall consensus.
 */
class PrimosEventPublisher implements PrimosListener {

    private final ApplicationEventPublisher events;

    PrimosEventPublisher(ApplicationEventPublisher events) {
        this.events = events;
    }

    @Override
    public void onRoleChanged(Role previous, Role current, long term) {
        events.publishEvent(new PrimosEvents.RoleChanged(previous, current, term));
    }

    @Override
    public void onLeaderChanged(Optional<Leader> leader, long term) {
        events.publishEvent(new PrimosEvents.LeaderChanged(leader, term));
    }

    @Override
    public void onLeaderVersion(Leader leader, long term, long version) {
        events.publishEvent(new PrimosEvents.LeaderVersion(leader, term, version));
    }

    @Override
    public void onPeerUp(Peer peer) {
        events.publishEvent(new PrimosEvents.PeerUp(peer));
    }

    @Override
    public void onPeerDown(Peer peer) {
        events.publishEvent(new PrimosEvents.PeerDown(peer));
    }
}
