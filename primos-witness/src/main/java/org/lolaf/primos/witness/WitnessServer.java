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
package org.lolaf.primos.witness;

import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Role;
import org.lolaf.primos.Timings;
import org.lolaf.primos.core.ElectionNode;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageDecoder;
import org.lolaf.primos.wire.MessageType;
import org.lolaf.primos.wire.WireFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The standalone witness: one socket, one thread, and an ordinary voter per served application.
 *
 * <p>Nothing here is a second implementation of Raft. Each application gets an
 * {@link ElectionNode} configured {@code asWitness()} — the same role state machine, the same
 * codec, the same store and the same invariants the deterministic simulation exercises. What this
 * class adds is multi-tenancy: read the application name off a datagram, hand it to that
 * application's node, and keep the tenants from being able to hurt each other.
 *
 * <h2>Reading the name before verifying the tag</h2>
 *
 * <p>Every other reader in primos verifies the authentication tag before parsing anything,
 * unconditionally. <strong>The witness cannot.</strong> Its secret is per-application, so it must
 * know which application a datagram claims to be for before it knows which key to check it with,
 * and that claim is in the datagram's own unverified bytes.
 *
 * <p>That is sound, and the reasoning has to be written down rather than rediscovered. The peek is
 * a fixed-offset, fixed-width field read — no allocation sized by attacker input, nothing that can
 * be walked off the end. An attacker may claim any name; they still cannot produce a tag that
 * application's key accepts, and the node's own decoder re-checks the name after verifying, so
 * routing and acceptance cannot disagree. All an unauthenticated claim can do is choose which
 * bucket drops the datagram.
 *
 * <h2>Tenants cannot hurt each other</h2>
 *
 * <ul>
 *   <li><strong>State is a directory each</strong>, so one cluster's vote cannot be spent by
 *       another. Sharing a bucket is not an inconvenience here, it is a double vote.</li>
 *   <li><strong>Inboxes are bounded</strong>, so a flood aimed at one application cannot grow the
 *       server's heap.</li>
 *   <li><strong>A failed node is quarantined</strong>, not fatal. The realistic cause is a disk
 *       that can no longer make a vote durable; that application stops being served and the rest
 *       keep going, because one tenant's broken storage must not take down every other tenant's
 *       failover.</li>
 * </ul>
 *
 * <h2>Deployment</h2>
 *
 * <p>Two rules, both safety rather than advice. <strong>Never run two instances behind a virtual
 * address</strong> — two witnesses answering for one identity is a double vote by construction —
 * and <strong>never wipe the state directory</strong>, including by putting it on an ephemeral
 * container volume. A witness that forgets a vote produces exactly the split brain it was deployed
 * to prevent.
 *
 * <p>One thread, like a node. {@link #stop()} is the only method another thread may call.
 */
@Slf4j
public final class WitnessServer implements AutoCloseable {

    /**
     * One byte more than a datagram may legitimately be, so an oversized one is truncated into
     * failing the size check rather than being quietly accepted as a shorter message.
     */
    private static final int RECEIVE_CAPACITY = WireFormat.MAX_DATAGRAM_BYTES + 1;

    /**
     * How many distinct unknown names are remembered for once-per-name logging. Bounded because
     * the names come off the network: without a cap, naming a fresh application on every datagram
     * would be a memory leak an attacker controls.
     */
    private static final int MAX_REMEMBERED_NAMES = 1024;

    /**
     * An upper bound on one blocking select, so {@link #stop()} is never waited out for long.
     */
    private static final long MAX_WAIT_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final ConfigStore configStore;
    private final MonotonicClock clock;
    private final Random random;
    /**
     * A programmatic override for every served application's intervals, or null to use each
     * application's own.
     *
     * <p>Configuration is the ordinary way to set these, per application, so that one tenant's
     * profile is not every tenant's. This exists for a caller constructing a server in code —
     * chiefly a test driving nodes on deliberately fast timings — and it deliberately wins over the
     * files, because a caller that passed it meant it.
     */
    private final Timings timingsOverride;

    private final boolean voteLogging;
    private final int nodeId;
    private final StateStoreFactory stateStores;
    private final DatagramChannel channel;
    private final Selector selector;
    private final ByteBuffer receiveBuffer = ByteBuffer.allocate(RECEIVE_CAPACITY);
    private final Map<String, Served> served = new LinkedHashMap<>();
    private final Set<String> namesSeen = new HashSet<>();

    private volatile boolean stopped;
    private boolean closed;

    private long unroutable;
    private long unservedApplication;
    private long autoRegistrationsRefused;
    private long unusableName;
    private long droppedForFailedApplication;
    private long autoRegistrations;
    private long awaitingNodeConfig;
    private long registrationBadTag;

    /**
     * The ring an unserved name's first datagram is checked against, and the settings it came from.
     *
     * <p>Cached because {@code Mac} initialisation is the expensive half of an HMAC, and this runs
     * on datagrams naming applications nobody configured — which is exactly the traffic whose
     * volume an attacker chooses. The snapshot is swapped whole and only when something changed,
     * so comparing the reference is enough to know the keys are still the right ones.
     */
    private WitnessSettings admissionSettings;
    private final Map<String, MessageAuthenticator> admissionRings = new HashMap<>();

    private WitnessServer(Builder builder) {
        this.configStore = Objects.requireNonNull(builder.configStore, "configStore");
        this.clock = builder.clock;
        this.random = builder.random;
        this.nodeId = Peer.requireValidId(builder.nodeId, "nodeId");

        ConfigSnapshot snapshot = configStore.current();
        WitnessSettings settings = snapshot.settings();
        this.timingsOverride = builder.timings;
        this.voteLogging = settings.voteLogging();
        this.stateStores = builder.stateStores != null
                ? builder.stateStores
                : StateStoreFactory.perApplicationDirectory(settings.stateDirectory());

        try {
            this.channel = DatagramChannel.open();
            this.channel.configureBlocking(false);
            setIfSupported(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
            setIfSupported(StandardSocketOptions.SO_RCVBUF, builder.receiveBufferBytes);
            this.channel.bind(builder.bindAddress != null ? builder.bindAddress : settings.bindAddress());
            this.selector = Selector.open();
            this.channel.register(selector, SelectionKey.OP_READ);
        } catch (IOException e) {
            throw new UncheckedIOException("the witness cannot open its socket", e);
        }

        log.info("witness {} bound to {}, configuration from {}, state in {}",
                nodeId, boundAddress(), settings.configSource(), settings.stateDirectory());
        if (settings.configSource().isCluster()) {
            log.warn("this witness serves any application name it hears. That is a legitimate"
                    + " choice and it is also how a typo becomes a split brain: two clusters named"
                    + " 'orders' and 'ordres' would each get their own vote from it. Names that"
                    + " look like near misses are warned about individually.");
        }
        log.info("witness {}: {}", nodeId, settings.keyring().summary());
        if (settings.keyring().isEmpty()) {
            log.warn("no keys are configured. An unauthenticated witness answers anybody who asks,"
                    + " and one forged vote grant plus a candidate's own vote is a majority in a"
                    + " three-voter cluster — so this is a route to a false leader, not merely a"
                    + " denial of service.");
        }

        try {
            for (ApplicationConfig application : snapshot.applications().values()) {
                open(application);
            }
        } catch (RuntimeException e) {
            closeQuietly();
            throw e;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * The address actually bound, useful when the port was left to the operating system.
     */
    public InetSocketAddress boundAddress() {
        try {
            return (InetSocketAddress) channel.getLocalAddress();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the bound address", e);
        }
    }

    /**
     * Runs until {@link #stop()}.
     */
    public void run() {
        log.info("witness {} is serving {} application(s)", nodeId, served.size());
        while (!stopped) {
            runOnce(MAX_WAIT_NANOS);
        }
        log.info("witness {} has stopped", nodeId);
    }

    /**
     * One turn: wait for a datagram or the earliest deadline, route what arrived, let every node
     * run, then check the configuration.
     *
     * <p>Public so the deterministic simulation and the tests drive exactly the loop production
     * runs, rather than an approximation of it.
     */
    public void runOnce(long maxWaitNanos) {
        long now = clock.nanoTime();
        long wait = Math.min(maxWaitNanos, Math.max(0L, nextDeadlineNanos(now) - now));
        awaitReadable(wait);
        drainSocket();

        for (Served application : new ArrayList<>(served.values())) {
            application.node.runOnce(0L);
            for (Peer peer : application.transport.drainLearned()) {
                application.node.learnPeer(peer);
            }
            adoptStatedTimings(application);
            quarantineIfFailed(application);
        }

        if (configStore.poll(clock.nanoTime())) {
            applyConfiguration();
        }
    }

    /**
     * Makes a blocked {@link #run} return promptly. The one method another thread may call.
     */
    public void stop() {
        stopped = true;
        selector.wakeup();
    }

    /**
     * The applications being served right now, auto-registered ones included.
     */
    public Set<String> servedApplications() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(served.keySet()));
    }

    /**
     * What role one served application's node is in.
     *
     * <p>Always {@code FOLLOWER} on a healthy witness — it never campaigns and never leads — so
     * this exists for tests and for a diagnostic, not for a decision. Counters left with
     * {@code PrimosStats}: a witness registers a {@link org.lolaf.primos.PrimosStatsListener} per
     * served application instead, tagged by name like everything else it does.
     */
    public Role role(String application) {
        return require(application).node.role();
    }

    /**
     * Why datagrams were not handed to any application, and what could not be answered.
     */
    public RoutingStats routing() {
        long overflowed = 0;
        long undeliverable = 0;
        for (Served application : served.values()) {
            overflowed += application.transport.overflowed();
            undeliverable += application.transport.sendsDropped();
        }
        return new RoutingStats(unroutable, unservedApplication, autoRegistrationsRefused,
                unusableName, droppedForFailedApplication, autoRegistrations, awaitingNodeConfig,
                registrationBadTag, overflowed,
                undeliverable);
    }

    /**
     * The term and vote one application currently holds, for tests and diagnostics.
     */
    public long currentTerm(String application) {
        Served found = require(application);
        return found.node.currentTerm();
    }

    /**
     * Which node this application's vote is currently spent on, or -1.
     */
    public int votedFor(String application) {
        Served found = require(application);
        return found.node.votedFor();
    }

    private Served require(String application) {
        Served found = served.get(application);
        if (found == null) {
            throw new IllegalArgumentException("this witness does not serve '" + application + "'");
        }
        return found;
    }

    // ---------------------------------------------------------------- the loop

    private long nextDeadlineNanos(long now) {
        long deadline = configStore.nextCheckNanos(now);
        for (Served application : served.values()) {
            deadline = Math.min(deadline, application.node.nextDeadlineNanos());
        }
        return deadline;
    }

    private void awaitReadable(long waitNanos) {
        try {
            if (waitNanos <= 0) {
                selector.selectNow();
            } else {
                // select(0) blocks forever in NIO, so a sub-millisecond wait is rounded up rather
                // than turned into an indefinite one.
                selector.select(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(waitNanos)));
            }
            selector.selectedKeys().clear();
        } catch (IOException e) {
            log.debug("witness {}: select failed: {}", nodeId, e.toString());
        }
    }

    private void drainSocket() {
        while (!closed) {
            receiveBuffer.clear();
            SocketAddress source;
            try {
                source = channel.receive(receiveBuffer);
            } catch (IOException e) {
                log.debug("witness {}: receive failed: {}", nodeId, e.toString());
                return;
            }
            if (!(source instanceof InetSocketAddress)) {
                return;
            }
            receiveBuffer.flip();
            route(receiveBuffer, (InetSocketAddress) source);
        }
    }

    /**
     * Hands one datagram to the application it names.
     *
     * <p>The name is read from unverified bytes; see this class's own documentation for why that is
     * safe and why there is no alternative.
     */
    private void route(ByteBuffer datagram, InetSocketAddress source) {
        String application = MessageDecoder.peekApplicationName(datagram);
        if (application == null) {
            unroutable++;
            return;
        }
        Served target = served.get(application);
        if (target == null) {
            target = admit(application, datagram, MessageDecoder.peekMessageType(datagram),
                    source);
            if (target == null) {
                return;
            }
        }
        if (target.failed) {
            droppedForFailedApplication++;
            return;
        }
        target.transport.offer(datagram, source);
    }

    /**
     * Decides what to do with a name this witness is not currently serving.
     *
     * @return the application now serving it, or null if the datagram is to be dropped
     */
    private Served admit(String application, ByteBuffer datagram, MessageType claimed,
                         InetSocketAddress source) {
        boolean firstSighting = rememberName(application);
        WitnessSettings settings = configStore.current().settings();

        if (!ApplicationNames.isUsableAsDirectory(application)) {
            unusableName++;
            if (firstSighting) {
                log.warn("dropping traffic for application '{}' from {}: the name cannot be a"
                                + " directory name, and each application's state is a directory",
                        application, source);
            }
            return null;
        }
        if (!settings.configSource().isCluster()) {
            unservedApplication++;
            if (firstSighting) {
                warnUnserved(application, source);
            }
            return null;
        }
        // Only a node's own statement of its configuration may bring an application into
        // existence. A Ping or a RequestVote would create one with no timings to serve it by, so
        // the witness would run that tenant on its own defaults until an advertisement happened to
        // arrive — a window in which the vote it exists to cast is governed by the wrong numbers.
        // Waiting for the config message makes that window unrepresentable rather than short.
        if (claimed != MessageType.NODE_CONFIG) {
            awaitingNodeConfig++;
            if (firstSighting) {
                log.info("heard {} from application '{}' at {}, which nothing configures. Holding"
                                + " off until one of its nodes states its configuration; that is"
                                + " what creates an application here.",
                        claimed == null ? "an unreadable datagram" : claimed, application, source);
            }
            return null;
        }
        // Nothing is created from bytes nobody has proved they could have written. Until this
        // check, the cluster application limit was the only thing between whoever could reach the
        // port and a state directory per datagram — which this plan called a bound on how full a
        // witness could get, when it was really the whole of the admission control.
        //
        // The keys tried are the ones that claim this name, or those claiming nothing when none
        // does, so a name somebody has reserved cannot be created by a holder of the general key.
        // That is what makes the keyring's reservation rule reach registration and not only
        // traffic for applications already being served.
        if (!admissionRing(application, settings).verify(datagram)) {
            registrationBadTag++;
            if (firstSighting) {
                log.warn("dropping a config message for unserved application '{}' from {}: no key"
                                + " that may speak for that name accepts it, so nothing is created."
                                + " Either that cluster holds a different key, or somebody who"
                                + " holds none is trying.",
                        application, source);
            }
            return null;
        }
        // verify() reads to the end of the datagram and the inbox copies from the current
        // position, so the buffer is handed on exactly as it arrived.
        datagram.position(0);
        if (served.size() >= settings.clusterApplicationLimit()) {
            autoRegistrationsRefused++;
            if (firstSighting) {
                log.warn("refusing to auto-register application '{}' from {}: this witness already"
                                + " serves {}, its configured limit. Raise config.cluster.limit if"
                                + " that is genuinely the size of the estate.",
                        application, source, settings.clusterApplicationLimit());
            }
            return null;
        }

        String nearMiss = ApplicationNames.nearMiss(application, served.keySet());
        log.warn("auto-registering application '{}', first heard from {}. Nobody configured it;"
                + " config.source=cluster is serving it anyway.", application, source);
        if (nearMiss != null) {
            log.warn("application '{}' is one or two characters from '{}', which this witness"
                            + " already serves. If that is a typo, the two clusters cannot see each"
                            + " other's datagrams and will each be granted their own vote here —"
                            + " which is two leaders where there should be one. Check both"
                            + " configurations before trusting this.",
                    application, nearMiss);
        }
        autoRegistrations++;
        // An auto-registered application has no file to say otherwise, so it takes the
        // witness-wide intervals. If those are wrong for it, its nodes will now say so.
        return open(new ApplicationConfig(application, settings.timings(), true));
    }

    /**
     * The keys an unserved name may be admitted by, built once per name per configuration.
     *
     * <p>With no key covering it there is nothing to check against and this admits whatever
     * arrives — the same thing that has always happened, and the reason running a witness without
     * keys is documented as something never to do. What it does not do is admit on the strength of
     * some <em>other</em> tenant's key.
     */
    private MessageAuthenticator admissionRing(String application, WitnessSettings settings) {
        if (settings != admissionSettings) {
            admissionRings.clear();
            admissionSettings = settings;
        }
        MessageAuthenticator cached = admissionRings.get(application);
        if (cached != null) {
            return cached;
        }
        List<WitnessKey> candidates = settings.keyring().candidatesFor(application);
        MessageAuthenticator ring;
        if (candidates.isEmpty()) {
            ring = MessageAuthenticator.disabled();
        } else {
            List<MessageAuthenticator.KeySpec> specs = new ArrayList<>(candidates.size());
            for (WitnessKey key : candidates) {
                specs.add(MessageAuthenticator.KeySpec.of(key.name(), key.algorithm(),
                        key.secret()));
            }
            ring = MessageAuthenticator.keyring(specs);
        }
        // Bounded the way the remembered-names set is, and for the same reason: these names arrive
        // from the network, so an unbounded map of them is a leak an attacker grows a datagram at
        // a time. Past the bound the ring is rebuilt each time, which costs work rather than heap.
        if (admissionRings.size() < MAX_REMEMBERED_NAMES) {
            admissionRings.put(application, ring);
        }
        return ring;
    }

    private void warnUnserved(String application, InetSocketAddress source) {
        String nearMiss = ApplicationNames.nearMiss(application, served.keySet());
        if (nearMiss != null) {
            log.warn("dropping traffic for unserved application '{}' from {}, which is one or two"
                    + " characters from '{}'. That is very likely a typo in one cluster's"
                    + " configuration; config.source=file is refusing it rather than granting it a"
                    + " vote of its own.", application, source, nearMiss);
        } else {
            log.warn("dropping traffic for unserved application '{}' from {}. Add"
                            + " applications/{}.properties to serve it.",
                    application, source, application);
        }
    }

    /**
     * @return whether this name had not been seen before, so it is worth a log line
     */
    private boolean rememberName(String application) {
        if (namesSeen.size() >= MAX_REMEMBERED_NAMES) {
            // Full. Stop logging rather than stop working: the counters still tell the story, and
            // the alternative is a set an attacker grows by naming a new application per datagram.
            return false;
        }
        return namesSeen.add(application);
    }

    // ---------------------------------------------------------- configuration

    private void applyConfiguration() {
        ConfigSnapshot snapshot = configStore.current();

        for (ApplicationConfig application : snapshot.applications().values()) {
            Served current = served.get(application.name());
            if (current == null) {
                open(application);
            } else if (!current.config.equals(application)) {
                rebuild(current, application);
            }
        }
        for (Served application : new ArrayList<>(served.values())) {
            if (snapshot.serves(application.name) || application.config.isAutoRegistered()) {
                continue;
            }
            // Its state stays on disk, deliberately. A cluster that comes back later still needs
            // the vote it was given remembered, and deleting it is the double-vote bug.
            log.info("no longer serving application '{}'; its state is left in place", application.name);
            served.remove(application.name);
            application.close();
        }
    }

    private Served open(ApplicationConfig application) {
        if (!ApplicationNames.isUsableAsDirectory(application.name())) {
            throw new IllegalArgumentException("application name '" + application.name()
                    + "' cannot be a directory name, and each application's state is a directory");
        }
        StateStore store = stateStores.open(application.name());
        if (!store.isDurable()) {
            store.close();
            throw new IllegalStateException("application '" + application.name() + "' was given a"
                    + " state store that does not survive a crash. A witness that forgets a vote"
                    + " can grant a second one in the same term and elect two leaders, which is"
                    + " precisely the failure it was deployed to prevent, so this is refused"
                    + " rather than warned about.");
        }
        Served opened = new Served(application, newTransport(application.name()), store);
        opened.node = newNode(application, opened.transport, store, null);
        served.put(application.name(), opened);
        opened.node.start();
        return opened;
    }

    /**
     * Replaces an application's node after its configuration changed, most often a rotated secret.
     *
     * <p>The node is rebuilt rather than mutated because the secret is baked into its codec, and
     * that is safe precisely because the node holds nothing that matters: the term and the vote
     * live in the store, which is kept and re-read, so a rebuilt node comes back knowing exactly
     * what it had promised. Learned addresses are kept too, in the transport, so replies keep
     * flowing without waiting for every node to speak again.
     */
    private void rebuild(Served application, ApplicationConfig updated) {
        if (log.isInfoEnabled()) {
            log.info("application '{}' was reconfigured", updated.name());
        }
        application.config = updated;
        application.failed = false;
        application.node = newNode(updated, application.transport, application.store,
                application.adoptedTimings);
        application.node.start();
        for (Map.Entry<Integer, InetSocketAddress> known : application.transport.endpoints()) {
            application.node.learnPeer(Peer.application(known.getKey(),
                    known.getValue().getHostString(), known.getValue().getPort()));
        }
    }

    /**
     * Which profile this application's node runs, in the order that settles who wins.
     *
     * <p>The builder override first, because a caller assembling a server in code — a test driving
     * deliberately fast intervals among them — means it and has no file to say so. Then what the
     * cluster stated, which is the point of {@link ConfigSource#CLUSTER}. Then the files.
     */
    private Timings timings(ApplicationConfig application, Timings adopted) {
        if (timingsOverride != null) {
            return timingsOverride;
        }
        return adopted != null ? adopted : application.timings();
    }

    /**
     * Follows a cluster's own intervals, when its operator has asked this witness to.
     *
     * <p>This is the whole of what {@link ConfigSource#CLUSTER} buys beyond registration: a tenant
     * retunes its own nodes and its witness follows, with nobody editing a file here. The witness
     * is the one member of a cluster whose configuration is owned by the wrong party — the tenant
     * owns the profile, the witness operator owns the file — and this is what closes that.
     *
     * <p>The rebuild is the existing reconfiguration path, so it keeps the state store and every
     * learned address: the term and the vote are on disk and are re-read, so the node comes back
     * knowing exactly what it had promised. Nothing here can move a term or spend a vote.
     */
    private void adoptStatedTimings(Served application) {
        if (!follows()) {
            return;
        }
        Timings stated = application.node.observedTimings();
        if (stated == null || stated.equals(application.adoptedTimings)) {
            return;
        }
        log.info("application '{}' states its own intervals — heartbeat {}ms, election {}-{}ms,"
                        + " probe {}ms, failure threshold {}. Following them.",
                application.name, stated.heartbeatInterval().toMillis(),
                stated.electionTimeoutMin().toMillis(), stated.electionTimeoutMax().toMillis(),
                stated.probeInterval().toMillis(), stated.failureThreshold());
        application.adoptedTimings = stated;
        rebuild(application, application.config);
    }

    private RoutedTransport newTransport(String application) {
        return new RoutedTransport(application, channel, selector);
    }

    private ElectionNode newNode(ApplicationConfig application, RoutedTransport transport,
                                 StateStore store, Timings adopted) {
        ClusterConfig.Builder config = ClusterConfig.builder()
                .applicationName(application.name())
                .nodeId(nodeId)
                .asWitness()
                .learnedMembership()
                .timings(timings(application, adopted))
                .voteLogging(voteLogging);
        if (follows()) {
            config.followsStatedTimings();
        }
        return new ElectionNode(config.build(), store, transport, clock, random,
                authenticatorFor(application));
    }

    /**
     * Whether this witness applies what its clusters state, rather than only reporting it.
     *
     * <p>The builder override is part of the question, not beside it: a caller that passed
     * intervals in code gets them whatever any datagram says, so nothing is being followed and a
     * mismatch is a real one worth reporting.
     */
    private boolean follows() {
        return timingsOverride == null
                && configStore.current().settings().configSource().isCluster();
    }

    /**
     * The key one application's datagrams are checked with.
     *
     * <p>The keys that may speak for this application:
     * otherwise — the keys that claim this name, or the ones claiming nothing when none does.
     *
     * <p>A ring rather than a single key, so a rotation is a key added to the file and removed
     * later rather than a flag day, and so a witness may hold a key per tenant without an
     * application file existing for any of them.
     */
    private MessageAuthenticator authenticatorFor(ApplicationConfig application) {
        List<WitnessKey> candidates =
                configStore.current().settings().keyring().candidatesFor(application.name());
        if (candidates.isEmpty()) {
            log.warn("application '{}' is served without authentication: no configured key claims"
                    + " it. Anyone who can reach this port can forge a vote grant for it, and one"
                    + " forged grant plus a candidate's own vote is a majority in a three-voter"
                    + " cluster.", application.name());
            return MessageAuthenticator.disabled();
        }
        List<MessageAuthenticator.KeySpec> ring = new ArrayList<>(candidates.size());
        for (WitnessKey key : candidates) {
            ring.add(MessageAuthenticator.KeySpec.of(key.name(), key.algorithm(), key.secret()));
        }
        return MessageAuthenticator.keyring(ring);
    }

    /**
     * Stops serving an application whose node has failed, without stopping the server.
     *
     * <p>The realistic cause is storage that can no longer make a vote durable. Continuing to
     * answer for that application would mean voting on state it may forget, so it stops; every
     * other tenant keeps its failover, because one broken disk must not be a shared outage.
     */
    private void quarantineIfFailed(Served application) {
        if (application.failed || !application.node.isFailed()) {
            return;
        }
        application.failed = true;
        log.error("application '{}' has failed and is no longer being served by this witness."
                        + " Every other application is unaffected. Any cluster relying on this"
                        + " witness is back to the fault tolerance it has without one until the"
                        + " cause is fixed and the witness restarted.",
                application.name, application.node.failureCause());
    }

    // ---------------------------------------------------------------- closing

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        stopped = true;
        closeQuietly();
    }

    private void closeQuietly() {
        for (Served application : served.values()) {
            application.close();
        }
        served.clear();
        try {
            configStore.close();
        } catch (RuntimeException e) {
            log.debug("closing the config store failed", e);
        }
        try {
            selector.wakeup();
            channel.close();
        } catch (IOException e) {
            log.debug("closing the channel failed", e);
        }
        try {
            selector.close();
        } catch (IOException e) {
            log.debug("closing the selector failed", e);
        }
    }

    private <T> void setIfSupported(SocketOption<T> option, T value) {
        try {
            channel.setOption(option, value);
        } catch (IOException | UnsupportedOperationException | IllegalArgumentException e) {
            log.debug("this platform does not support {}: {}", option.name(), e.toString());
        }
    }

    /**
     * One served application: its configuration, its socket view, its state and its voter.
     */
    private static final class Served {

        private final String name;
        private final RoutedTransport transport;
        private final StateStore store;

        private ApplicationConfig config;
        private ElectionNode node;
        private boolean failed;

        /**
         * The profile this cluster stated, once it has, under {@link ConfigSource#CLUSTER}.
         *
         * <p>Deliberately <em>beside</em> {@link #config} rather than in it. Configuration is what
         * the files say; this is what the cluster says, and keeping them apart is what stops the
         * two fighting — a reload would otherwise find the file's intervals differing from the
         * adopted ones, rebuild back to the file's, and be overwritten again by the next
         * advertisement, forever.
         */
        private Timings adoptedTimings;

        private Served(ApplicationConfig config, RoutedTransport transport, StateStore store) {
            this.name = config.name();
            this.config = config;
            this.transport = transport;
            this.store = store;
        }

        /**
         * Tells this application's cluster the witness is going away, then releases everything.
         *
         * <p>Worth the datagram. A cluster whose witness has silently gone is back to the fault
         * tolerance it has without one, and the sooner it says so the sooner somebody looks — a
         * two-node pair in that state tolerates zero failures and looks perfectly healthy.
         */
        private void close() {
            if (!failed && node != null) {
                try {
                    node.announceDeparture();
                } catch (RuntimeException e) {
                    log.debug("cluster '{}': could not announce the witness leaving: {}",
                            name, e.toString());
                }
            }
            transport.close();
            store.close();
        }
    }

    /**
     * Why datagrams did not reach an application.
     */
    @Value
    @AllArgsConstructor
    public static class RoutingStats {

        /**
         * Datagrams too short or too malformed to carry a readable application name.
         */
        long unroutable;

        /**
         * Datagrams naming an application this witness does not serve.
         */
        long unservedApplication;

        /**
         * Datagrams refused because the auto-registration limit was reached.
         */
        long autoRegistrationsRefused;

        /**
         * Datagrams naming something that could not be a directory name.
         */
        long unusableName;

        /**
         * Datagrams for an application whose node has failed and been quarantined.
         */
        long droppedForFailedApplication;

        /**
         * Applications that appeared by themselves. Under {@code ConfigSource.FILE} always zero.
         */
        long autoRegistrations;

        /**
         * Datagrams from an unserved application that were not its nodes stating their config.
         *
         * <p>Ordinary and transient while a new cluster starts: its probes and votes arrive before
         * the config message that creates it. Sustained, it means a cluster is talking to this
         * witness and never saying who it is — a version mismatch, or a name nobody configured
         * under {@code ConfigSource.FILE}.
         */
        long awaitingNodeConfig;

        /**
         * Config messages for an unserved application that no key covering it would accept.
         *
         * <p>The bound on who may create an application here. Sustained, it is either a cluster
         * holding a key this witness does not, or somebody holding none trying to make state
         * directories.
         */
        long registrationBadTag;

        /**
         * Datagrams dropped because an application's inbox was full.
         *
         * <p>A witness answers a handful of messages a second, so anything here means a flood
         * rather than a buffer that is too small — and the bound is what stops that flood being
         * every other tenant's problem too.
         */
        long inboxOverflowed;

        /**
         * Replies this witness never put on the wire, for any reason.
         *
         * <p>Expected to stay at zero, and worth alerting on if it does not. A witness that
         * silently fails to answer is indistinguishable from a witness nobody is asking, and the
         * cluster on the other end is the one that pays: it waits out an election timeout for a
         * vote that was never coming.
         */
        long undeliverableReplies;

    }

    /**
     * Builds a {@link WitnessServer}.
     */
    public static final class Builder {

        /**
         * Where the served set and the secrets come from. Required.
         */
        @Setter
        private ConfigStore configStore;

        private MonotonicClock clock = MonotonicClock.SYSTEM;
        private Random random = new Random();

        /**
         * Overrides the timing profile from the configuration.
         *
         * <p>A witness usually sits on a third network, where a WAN round trip makes the LAN
         * election timeout marginal; {@link Timings#wan()} is the profile for that.
         */
        @Setter
        private Timings timings;

        /**
         * This witness's node id, which every cluster must configure as its witness peer's id.
         *
         * <p>Defaults to {@link Peer#MAX_ID}, the top of the range, so that it does not collide
         * with application nodes numbered from zero upward. It is durable in every served
         * application's {@code votedFor}, so changing it on a running witness makes its recorded
         * votes unattributable — treat it as fixed once deployed.
         */
        @Setter
        private int nodeId = Peer.MAX_ID;

        /**
         * Where each application's term and vote are kept. Defaults to a directory each under the
         * configured state directory; whatever is supplied must be durable, or the server refuses
         * to start.
         */
        @Setter
        private StateStoreFactory stateStores;

        /**
         * Overrides the bind address from the configuration.
         */
        @Setter
        private InetSocketAddress bindAddress;

        /**
         * Socket receive buffer. Sized generously on purpose: a silently overflowing socket looks
         * exactly like a partitioned network, and on a witness it looks like every tenant at once.
         */
        @Setter
        private int receiveBufferBytes = 1 << 20;

        private Builder() {
        }

        /**
         * Only for tests: the deterministic simulation supplies a virtual clock.
         */
        public Builder clock(MonotonicClock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * Only for tests: a seeded source makes a run reproducible.
         */
        public Builder random(Random random) {
            this.random = Objects.requireNonNull(random, "random");
            return this;
        }

        public WitnessServer build() {
            return new WitnessServer(this);
        }
    }
}
