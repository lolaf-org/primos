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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;

import java.util.Objects;
import java.util.Optional;

/**
 * Who is currently believed to be leading the cluster.
 *
 * <p>This is not simply a {@link Peer}, and the reason is that the leader may be <em>this</em>
 * node. {@link Peer} is every member other than this one and carries an address to reach it by,
 * which a node does not hold for itself — so a bare {@code Optional<Peer>} would have to report
 * "I am the leader" as empty, the same value that means "nobody is leading". Those are opposite
 * situations and must not share an encoding.
 *
 * <p>So there are three states, and {@link #isSelf()} and {@link #peer()} tell them apart:
 *
 * <ul>
 *   <li><strong>this node leads</strong> — {@code isSelf()}, with no peer, because there is
 *       nowhere to forward work to;</li>
 *   <li><strong>a peer leads</strong> — {@code peer()} is present, and is where work goes;</li>
 *   <li><strong>a peer leads that is not identified yet</strong> — neither, leaving only the
 *       {@link #id()}. This arises on the witness server alone, which learns its voters from
 *       traffic and so may hear a leader's heartbeat before it has learned that node.</li>
 * </ul>
 *
 * <p>"No leader at all" is not one of them: it is the empty {@link Optional} the leader is handed
 * out in, and it is a real and common state during an election rather than an error.
 */
@Value
public class Leader {

    /**
     * The leader's node id, which is its identity in every case.
     */
    int id;

    /**
     * The leader's human-readable name, for logs. Derived from the id when nothing better is
     * known.
     */
    String name;

    @Getter(AccessLevel.NONE)
    boolean self;

    /**
     * Null when this node is the leader, and on a witness server when the leader has not been
     * learned yet. Read through {@link #peer()}.
     */
    @Getter(AccessLevel.NONE)
    Peer peer;

    private Leader(int id, String name, boolean self, Peer peer) {
        this.id = Peer.requireValidId(id, "leader id");
        this.name = Objects.requireNonNull(name, "name");
        this.self = self;
        this.peer = peer;
    }

    /**
     * This node is the leader.
     *
     * <p>The name is derived rather than configured, by the same rule {@link Peer} uses for a
     * member that was not given one. A node handing its own name back to the application that
     * configured it was a round trip that bought nothing — a peer's name saves a lookup the caller
     * may not be able to do, and its own never did.
     */
    public static Leader self(int id) {
        return new Leader(id, Peer.defaultName(id, PeerType.APPLICATION), true, null);
    }

    /**
     * A peer is the leader, and this is where work is forwarded to.
     */
    public static Leader at(Peer peer) {
        Objects.requireNonNull(peer, "peer");
        return new Leader(peer.id(), peer.name(), false, peer);
    }

    /**
     * A leader known only by its id — the witness server's case, described on the class.
     */
    public static Leader byId(int id) {
        return new Leader(id, "node-" + id, false, null);
    }

    /**
     * Whether this node is itself the leader. Then, and only then, {@link #peer()} is empty
     * without the leader being unknown.
     */
    public boolean isSelf() {
        return self;
    }

    /**
     * Where to reach the leader, present whenever it is an identified peer.
     *
     * <p>Empty means there is nowhere to forward work to, not that leadership is in doubt: either
     * this node holds it, or the peer holding it has not been identified yet.
     */
    public Optional<Peer> peer() {
        return Optional.ofNullable(peer);
    }
}
