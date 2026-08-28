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

import java.net.InetSocketAddress;
import java.util.Objects;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * One member of the cluster other than this node: its identity, its kind, and where to reach it.
 *
 * <p>Identity is the {@link #id()}, never the address. Addresses may be learned or re-resolved at
 * runtime, and on a multi-homed host the source address of a datagram need not be the one
 * configured, so nothing may key off the address.
 *
 * <p>The host is kept unresolved here on purpose: DNS resolution can block for seconds and must
 * never happen on the node thread.
 */
@Getter
@EqualsAndHashCode
@ToString
public final class Peer {

    /** Lowest permitted node id. */
    public static final int MIN_ID = 0;

    /**
     * Highest permitted node id.
     *
     * <p>This bounds the id <em>value</em>, not the size of a cluster — see
     * {@link Quorum#MAX_RECOMMENDED_VOTERS} for that, which is far smaller. Ids may be sparse, so
     * an operator is free to use a scheme that means something, such as {@code 101, 102, 201}
     * for two datacentres.
     *
     * <p>The wire carries the sender as a 32-bit integer, so the protocol would accept any
     * {@code int}. The cap is here for legibility and for catching mistakes: ids appear in every
     * log line and packet dump, and a four-digit ceiling makes a port number or an IP octet passed
     * by accident fail immediately rather than become a peer nobody can find.
     */
    public static final int MAX_ID = 1_023;

    private final int id;
    private final String name;
    private final PeerType type;
    private final String host;
    private final int port;

    private Peer(int id, String name, PeerType type, String host, int port) {
        this.id = requireValidId(id, "peer id");
        this.type = Objects.requireNonNull(type, "type");
        this.host = requireHost(host);
        this.port = requirePort(port);
        this.name = (name == null || name.trim().isEmpty()) ? defaultName(id, this.type) : name.trim();
    }

    /** An application peer with a generated name. */
    public static Peer application(int id, String host, int port) {
        return new Peer(id, null, PeerType.APPLICATION, host, port);
    }

    /** An application peer with a human-readable name used in logs. */
    public static Peer application(int id, String name, String host, int port) {
        return new Peer(id, name, PeerType.APPLICATION, host, port);
    }

    /** A witness peer with a generated name. */
    public static Peer witness(int id, String host, int port) {
        return new Peer(id, null, PeerType.WITNESS, host, port);
    }

    /** A witness peer with a human-readable name used in logs. */
    public static Peer witness(int id, String name, String host, int port) {
        return new Peer(id, name, PeerType.WITNESS, host, port);
    }

    /** Whether this peer is a witness, and therefore never a leader and never a work destination. */
    public boolean isWitness() {
        return type.isWitness();
    }

    /**
     * The configured endpoint, deliberately left unresolved. Resolution happens in the transport,
     * off the node thread.
     */
    public InetSocketAddress unresolvedAddress() {
        return InetSocketAddress.createUnresolved(host, port);
    }

    /** Validates a node id, returning it unchanged, for reuse by configuration. */
    public static int requireValidId(int id, String what) {
        if (id < MIN_ID || id > MAX_ID) {
            throw new IllegalArgumentException(
                    what + " must be between " + MIN_ID + " and " + MAX_ID + ", but was " + id);
        }
        return id;
    }

    private static String requireHost(String host) {
        Objects.requireNonNull(host, "host");
        if (host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        return host.trim();
    }

    private static int requirePort(int port) {
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535, but was " + port);
        }
        return port;
    }

    private static String defaultName(int id, PeerType type) {
        return (type.isWitness() ? "witness-" : "node-") + id;
    }
}
