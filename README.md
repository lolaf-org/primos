# Primos

Raft leader election for clustered Java applications. Every instance knows whether it is the
leader or a follower, and is told the moment that changes.

*Primus inter pares* — first among equals. Also *cousins* in Spanish and Portuguese: peers, one
of whom is currently first.

> **Status: built and green, not yet published.** The library and the witness server are complete
> and tested; what remains is documentation and the first release to Maven Central. Every design
> decision, and the reasoning behind it, is in [PLAN.md](PLAN.md).

## What it is

```java
List<Peer> peers = Arrays.asList(
        Peer.application(2, "node-b", 7420),
        Peer.application(3, "node-c", 7420));

ClusterConfig cluster = ClusterConfig.builder()   // who is in the cluster
        .applicationName("orders")
        .nodeId(1)
        .peers(peers)
        .build();

PrimosNode node = PrimosNode.builder()            // and how this node runs
        .clusterConfig(cluster)
        .clusterSecret(secret)
        // Suppliers, not instances: each start binds a socket and takes the state lock, so a
        // node that stops can start again. The transport builder is itself the supplier.
        .store(() -> new FileStateStore(Paths.get("/var/lib/orders/primos")))
        .transport(UdpTransport.builder()
                .bindAddress(new InetSocketAddress(7420))
                .peers(peers)
                .addressLearning(true))       // only ever with a secret set
        .listener(new PrimosListener() {
            @Override
            public void onRoleChanged(Role previous, Role current, long term) {
                log.info("now {} in term {}", current, term);
            }
        })
        .build();

node.start();
```

Two objects because they answer different questions. The `ClusterConfig` describes the cluster and
is the same shape on every node — it is also where a cluster's shape is validated, once. The node
builder wires up this node's runtime around it: where its state lives, how it reaches the network,
who listens.

Every listener method has a default, so override only what you need: `onRoleChanged`,
`onLeaderChanged`, `onPeerUp`, `onPeerDown`.

`store` and `transport` are required and have no defaults — a node that cannot remember its vote
across a restart can elect two leaders, so there is no convenient in-memory fallback to reach for
by accident. `addressLearning` should only be enabled with authentication on: without a tag, one
spoofed datagram would redirect a peer's traffic and partition it without forging any content.

These examples are compiled and run as a test ([`ReadmeExamplesTest`](primos/src/test/java/org/lolaf/primos/ReadmeExamplesTest.java)),
so an API change that makes this section wrong breaks the build.

## What it is not

**Leader election only.** No replicated log, no state machine, no snapshots, no runtime membership
changes. Your application keeps its own state; primos only tells it who is currently in charge.
There is no intention to extend it to replication.

## Using it safely

**This is the section most likely to prevent an incident.**

There is always a window in which your application believes it is leader and is not. That is
inherent to leader election, not a defect here: leadership can be lost at any instant, and the news
travels no faster than the network. CheckQuorum bounds the window; nothing can remove it.

So `role() == LEADER` is **not** a sufficient guard for an external side effect. Carry the term
with the work:

```java
RoleAndTerm current = node.roleAndTerm();   // read together, never separately
if (current.isLeader()) {
    database.write(record, current.term()); // and have the database reject a stale term
}
```

The external system rejects anything carrying a term lower than the highest it has seen. That is
what makes a stale leader's write *fail* rather than corrupt. Anything guarded only by a boolean
will eventually be wrong.

Two smaller rules for the same section:

- **Callbacks arrive on the node thread.** Hand real work to your own executor, or you are holding
  up heartbeats. A callback slower than half the heartbeat interval is logged as such.
- **A node can leave the cluster and rejoin.** `stop()` then `start()` works: the builder takes
  *suppliers* of the store and transport, so each start makes a fresh pair. That is the reason for
  the shape — a `FileStateStore` takes its exclusive lock in its constructor and a `UdpTransport`
  binds its socket in its own, so a stopped node holds a released lock and a closed channel and
  restarting needs new instances rather than a reset. It is safe because the term and the vote are
  on disk: a node that comes back re-reads both and cannot forget a vote it granted. A `FAILED`
  node is never restartable.
- **`FAILED` is terminal.** A node whose store can no longer make a vote durable stops claiming
  leadership permanently, because continuing would mean voting on state it may forget. Treat the
  event as a signal to restart the process, not to wait.

## Shutting down cleanly

A node that simply stops is *correct* — the cluster recovers on its own timers — but it is slow in
two different ways, and primos closes both. Both are best-effort: one datagram, nothing waits for
it, and if it is lost the cluster falls back to exactly the behaviour it would have had anyway.

**A departing leader hands over.** Stopping a leader would otherwise leave the cluster with nobody
in charge until a follower's election timeout expires — 300–600ms on the LAN defaults, on every
deploy and every restart. Instead it sends `TimeoutNow` (Raft thesis §3.10) to the follower it has
most recently exchanged a round trip with, telling it to campaign immediately. Failover becomes
about one round trip. The witness is never chosen, and would refuse anyway.

**Any departing node announces itself.** Reachability is otherwise inferred from silence, and on
the JVM silence has to be given a long benefit of the doubt: a stop-the-world collection looks
exactly like a dead process, so the probe timeout gets tuned up — often to tens of seconds — and a
node that shut down politely stays reported as alive for that whole window. Lowering the timeout
reintroduces the false positives the tuning existed to prevent, so the fix is not a shorter
timeout but a message: a shutdown is the one departure a node knows about in advance and can say
out loud. `onPeerDown` then fires at once instead of a timeout later.

Crashes, kills, power loss and partitions still take the probe timeout, as they must. The timeout
stays long; it just stops being paid for the common case.

**Neither message changes the voter set.** Quorum is always computed from configured membership
and never from who happens to be reachable — five voters with three gone still need three votes.
Being told a peer is leaving is a hint, and measurement stays authoritative: a peer that announces
and then answers a probe anyway is reported up again.

## The witness

A two-node active/passive pair is the case this exists for. Two nodes alone form a 2-voter cluster
with quorum 2, which tolerates **zero** failures — any death or partition leaves no leader at all.
A **witness** is a voter that never campaigns and never leads, deployed in a third failure domain.
It makes the pair 3 voters with quorum 2: whichever node still reaches the witness has a majority
and leads, the other stands down, and there is no split brain.

It is an ordinary voter that declines to campaign — it sits in the voter set, persists
`currentTerm`/`votedFor`, and answers `RequestVote` like any node. That is deliberately the boring
choice: it stays inside Raft's existing correctness argument rather than bolting a second
consensus mechanism onto the first.

| configuration | voters | quorum | tolerates |
|---|---|---|---|
| 1 node | 1 | 1 | 0 — and see below |
| 2 nodes | 2 | 2 | **0** |
| 2 nodes + witness | 3 | 2 | 1 |
| 3 nodes | 3 | 2 | 1 |
| 3 nodes + witness | 4 | 3 | **1** |

A witness helps even-sized clusters and hurts odd-sized ones, so primos **warns and continues**
rather than refusing — every node logs a quorum summary at startup, plus a warning when the
configuration is strictly dominated by an alternative.

### One node leads itself, permanently

**A node configured with no peers is a one-voter cluster, and it becomes the leader.** Quorum is
one, so it elects itself about an election timeout after starting, and then nothing can depose it:
there is no majority for it to lose contact with, so the check that stands a partitioned leader down
never fires. It stays leader for as long as the process runs.

That is deliberate and supported. It makes development and testing trivial, and it lets an
application run the same code path in a single-instance deployment instead of branching around
primos entirely.

**It is also exactly what a peer list that failed to load looks like** — a mistyped property, an
environment variable that did not interpolate, a YAML block at the wrong indentation. Made once, you
get a node that quietly leads alone. Made on every node of an intended cluster, you get one
permanent leader per node, each of them reporting perfect health and each of them right about its
own arithmetic. Nothing in the protocol can detect this, because from inside, a one-node cluster is
behaving correctly.

So primos warns at startup whenever it is configured this way:

```
cluster 'orders': 1 voter (1 node), quorum 1, tolerates 0 failures
cluster 'orders': one node and no witness, so this node is the only voter. It elects itself about
one election timeout after starting and leads for as long as it runs, and nothing can depose it —
there is no majority to lose contact with. That is a legitimate single instance deployment, and it
is also what a mistyped or unread peer list looks like, in which case every node of the intended
cluster is doing this at once and each of them is the leader. Check that peers were meant to be
omitted.
```

If you meant it, the warning is one line at startup. If you did not, it is the only thing that will
tell you.

### The witness server

`primos-witness` is the witness server, serving many clusters at once, keyed by application name.
It is a **library** — an ordinary jar that inherits nothing but `slf4j-api` — and the runnable
packagings are their own modules: [`primos-witness-cli`](primos-witness-cli), a `tar.gz` beside a
systemd unit, and [`primos-witness-boot`](primos-witness-boot), a Spring Boot application and a
container image. Neither front door holds logic of its own.

**Operators want [the witness README](primos-witness/README.md)** — configuring, key rotation,
configuration sources, what to alert on, and troubleshooting, all of it true whichever front door
runs it. Installing one is documented with that front door. In brief, the tarball ships a launcher,
its jars and a commented sample configuration:

```
bin/primos-witness --config /etc/primos-witness
```

```
/etc/primos-witness/witness.properties            # port, config source, state directory, keys
/etc/primos-witness/applications/orders.properties
/etc/primos-witness/applications/billing.properties
```

The application name comes from the **filename**, so it cannot disagree with the file's contents.
Adding a cluster is a file drop, and the directory is re-read while the server runs — onboarding
never needs a restart, which matters because a restart is the one genuinely risky operation in a
witness's lifecycle: while it is down, every two-node cluster it serves is back to tolerating zero
failures.

Three rules are safety rather than advice:

- **Never run two instances behind a virtual address or load balancer.** Two witnesses answering
  for one identity is a double vote by construction.
- **Never wipe the state directory**, including by putting it on an ephemeral container volume. A
  witness that forgets a vote can grant a second one in the same term and elect two leaders — the
  exact failure it was deployed to prevent. It refuses to start on a non-durable store.
- **Never run without a secret.** Authentication protects *replies* as much as requests: one
  forged vote grant plus a candidate's own vote is a majority in a three-voter cluster, so an
  unauthenticated witness link is a route to a false leader, not merely a denial of service.

**Keys live in `witness.properties`, as a keyring rather than one secret**, and never in an
application's own file — a cluster that arrives over the network has no file to have had a secret
set in. Each key may name the applications it covers, with `*` patterns, and the rule is that a
name any key claims is verified only by the keys claiming it. So the same setting spans one shared
key for a fleet, a dedicated key per tenant, and everything between. Rotation is list membership:
add the new key, roll the nodes, remove the old.

**`config.source` defaults to `file`**, and here is why. Under `cluster` the witness serves any
name whose nodes state a configuration it can verify. Suppose two clusters are configured `orders`
and `ordres`. They drop each other's datagrams for a mismatched name, so each sees the other as
unreachable and asks the witness instead. The witness auto-creates *two* buckets and grants a vote
from each — **two leaders out of one typo**, with the cross-talk protection hiding the cause.
`cluster` is a legitimate choice and is hardened for it (WARN on every auto-registration, near-miss
name detection, and a cap so an open witness cannot be filled up), but `file` is the default for
that reason.

## Observability

`slf4j-api` is the only dependency, so there is no Micrometer and no metrics registry. The node
tells you what it did, as it does it, and you increment whatever you already run:

```java
PrimosNode.builder()
        .statsListener(new PrimosStatsListener() {
            @Override
            public void onElectionWon(long term, long campaignNanos) {
                failovers.record(campaignNanos);      // how long the failover actually took
            }

            @Override
            public void onProbeRoundTrip(int peerId, long nanos) {
                rtt.record(nanos);                     // every sample, so a p99 is possible
            }

            @Override
            public void onDatagramDropped(DropReason reason) {
                drops.increment(reason);
            }
        })
        // ...
```

**There is no `stats()` snapshot, deliberately.** A snapshot cannot carry the three things worth
having — a *distribution*, since a p99 needs every sample and no polling interval recovers one from
a last-value gauge; a *duration that begins and ends between two samples*, like how long a campaign
took or how long the cluster had no leader, which are the two numbers most wanted after a failover
and are gone before anybody looks; and an *event already superseded*, like leadership going A to B
to A inside one interval, which is invisible to a poller and is exactly the flap worth alerting on.
Everything a snapshot *could* have carried, it carried twice — the node counted, and any listener
counted the same events again.

What you branch on is still a plain call: `role()`, `roleAndTerm()`, `leader()`, `isRunning()`.

**The contract is harder than `PrimosListener`'s: callbacks must not block *and* must not
allocate.** They run on the node thread and some of them run per datagram, so every method takes
primitives and enums, there is one sink rather than a list, and there is no executor — an iterator
or a `Runnable` per drop is the allocation the shape exists to avoid. Compose with
`PrimosStatsListener.of(a, b)`. A listener that throws is caught and logged: a metrics binder must
not be able to stop consensus. There is a test asserting a counting listener adds **zero** bytes a
turn on the drop path, because otherwise the rule is only a comment.

`primos-micrometer` is exactly such a listener, and the library still names no third-party type.
One dependency binds a node's meters onto whatever `MeterRegistry` you already run — the meter to
alert on being `primos.leader`, a 0/1 gauge that sums to 1 across a healthy cluster. See that
module's README for the catalogue.

### Mismatched timings are reported

Every node restates its intervals to its peers, and a peer whose intervals differ is logged at ERROR with both sets
side by side, and reported to `onTimingMismatch`. Coming back into agreement is logged too, because during a
rolling change half a cluster disagrees by design and you need to see it end.

The cadence follows reachability rather than a fixed clock: **while any peer has not answered, a node restates itself
every probe interval; once they all have, every advertisement interval** (30s by default). One statement then thirty
seconds of silence is the right steady state and the wrong bootstrap — this is UDP, and a lost first statement is
exactly the case that most needs another.

**Between nodes it reports and never acts.** A mismatch makes failover slower, never unsafe — a node with a longer
election timeout is proportionally more reluctant to grant the pre-vote a failover waits on — so nothing about the
cluster's behaviour changes when one is found. A node that refused a vote over a mismatch would turn a rolling timing
change into a partition, which is worse than the problem.

A witness server is the one exception, and only when its operator has asked for it: under
`config.source=cluster` it *follows* the intervals its clusters state instead of reporting them, which is what lets a
tenant retune without anybody editing the witness. The argument above does not carry over because it is an argument
about nodes chasing each other during a rolling change, and a witness is not a member of that conversation — see the
[witness README](primos-witness/README.md).

### What gets reported

| | |
|---|---|
| `onElectionWon` / `onElectionLost` | with how long the campaign took, timed from the pre-vote |
| `onLeaderKnown` | with how long the cluster had none, across every election in the outage |
| `onLeadershipRelinquished` | CheckQuorum stood this node down — it could not reach a majority |
| `onHandoverSent` / `onHandoverAccepted` | leadership handed on at shutdown, and taken up |
| `onDepartureObserved` | a peer said it was going |
| `onProbeRoundTrip` | every sample, per peer |
| `onDatagramDropped` | with the reason |
| `onTimingMismatch`, `onUnusableNodeConfig`, `onPeerForgotten` | configuration a peer stated |

Two answer questions a rolling deploy raises immediately. `onHandoverSent` against the number of
times this node was stopped while leading is the count of deploys that cost the cluster an election
timeout. `onDepartureObserved` against the peer-down events separates orderly shutdowns from
departures merely inferred from silence — which on the JVM might have been a garbage collection.

Role, leader and peer up/down are **not** here: they are on `PrimosListener`, and a binder that
wants both implements both. Two ways to hear one thing is how two dashboards come to disagree.

The witness server registers one listener per served application, so a stray cluster on a
witness serving whatever it hears is visible on a dashboard.

## Timing

Two profiles ship. `Timings.lan()` is the default; `Timings.wan()` is for a cluster whose witness
sits on another network. The witness server takes the same choice as `timings.profile = lan | wan`.

| parameter | `lan()` | `wan()` | what it controls |
|---|---|---|---|
| heartbeat interval | 50ms | 200ms | how often a leader asserts itself |
| election timeout | 300–600ms | 1.5–3s | how long silence is tolerated before campaigning |
| probe interval | 1s | 2s | how often every node pings every peer |
| failure threshold | 3 | 3 | missed probes before `onPeerDown` |

**Choose by the round trip to the furthest voter.** That is the only question that matters, and for
a witness deployment the furthest voter is almost always the witness. Raft's governing inequality
is `broadcastTime << electionTimeout << MTBF`, with the election timeout an order of magnitude
above the round trip. A 50ms WAN round trip against a 300ms election timeout is only 6×, which is
why `lan()` is marginal the moment a voter is off-site.

**Use one profile for the whole cluster, witness included.** A node refuses to grant a *pre-vote*
while it has heard from a leader within `electionTimeoutMin`, so a witness on `wan()` inside an
otherwise-`lan()` cluster is five times more reluctant to agree to a failover than its peers —
which is exactly the vote a two-node pair is waiting on. Mixed profiles do not corrupt anything;
they just make failover slower in a way that is very hard to see.

### What the numbers cost you

**Failover after a crash** is roughly `electionTimeoutMax + 2 × RTT` — the randomised timeout has
to expire, then a pre-vote round, then the election. About **0.6s** on `lan()`, about **3.1s** on
`wan()`. Halving it means halving the election timeout, which buys spurious elections; there is no
free direction here.

**Failover after a clean stop** is about **one round trip**, because the departing leader hands
over rather than going silent — single-digit milliseconds on a LAN. Planned restarts do not pay the
timeout above; only genuine crashes do.

**A peer is reported down** after `probeInterval × failureThreshold`, plus up to one interval before
the first unanswered probe: **3–4s** on `lan()`, **6–8s** on `wan()`. A node that shuts down
cleanly is reported at once instead, because it says so.

### The JVM caveat, which is the one that actually bites

**A stop-the-world pause is indistinguishable from a dead process.** If the leader stalls for longer
than a follower's election timeout, the follower campaigns — a failover caused by garbage
collection, not by any failure. The `lan()` defaults assume worst-case pauses comfortably under
300ms, which is realistic for a modestly-sized heap on G1, or for ZGC or Shenandoah at almost any
size, and *not* realistic for a large heap that can still reach a full GC.

So: measure your worst-case pause rather than your average, and keep `electionTimeoutMin` well
above it — 3× is a reasonable starting point. Setting a 30s election timeout to be safe is a
legitimate choice on a machine with 30s pauses; it just means 30s failovers, and that trade should
be made deliberately rather than discovered.

The same reasoning applies to the probe interval and `failureThreshold` — that product is why a
GC-tolerant deployment might set 10s probes, and why departures are *announced* rather than left to
be inferred from silence.

**Storage latency is the other floor.** A vote is fsync'd before it is answered, so a store whose
worst-case sync approaches the election timeout will lose elections while writing. Local SSD is
tens to hundreds of microseconds and irrelevant here; a network filesystem or a throttled cloud
volume is neither. (There is no warning for this yet — see PLAN.md.)

### Deriving your own

```java
Timings.builder()
        .heartbeatInterval(Duration.ofMillis(200))
        .electionTimeoutMin(Duration.ofMillis(1_500))
        .electionTimeoutMax(Duration.ofMillis(3_000))
        .probeInterval(Duration.ofSeconds(2))
        .failureThreshold(3)
        .build();
```

1. Start from the round trip to the furthest voter, and the worst-case GC pause.
2. Set `electionTimeoutMin` to comfortably exceed both — an order of magnitude over the round trip,
   several times the pause.
3. Set the heartbeat to at most a fifth of that.
4. Set `electionTimeoutMax` to roughly twice the minimum. The spread is not decoration:
   randomisation per node per election is a hard requirement from Raft §5.2, because nodes that
   time out together campaign together, split the vote, and repeat.
5. Set the probe interval by how quickly you want `onPeerDown`, bounded by GC tolerance.

`build()` rejects what would thrash, rather than letting it be found in production: an election
timeout under 5× the heartbeat, a randomisation range narrower than one heartbeat, a probe interval
over 15s (a NAT pinhole commonly expires at 30s, and a hole closing between probes reads as a peer
flapping), a `failureThreshold` below 1, and any non-positive duration.

## Design in one page

- **Zero dependencies.** `slf4j-api` at compile scope and nothing else — Lombok is compile-time
  only and not transitive. Plain JDK concurrency and NIO throughout.
- **UDP transport.** Raft already assumes a network that drops, delays, duplicates and reorders,
  and recovers from all of it with its own timers, so TCP's retransmission is duplicated work.
  Nine message types, all fixed-size, the largest 104 bytes including its authentication tag —
  every one fits an unfragmented datagram.
- **Optional HMAC on every datagram**, verified before anything is parsed. HMAC-SHA256 by
  default; `.macAlgorithm("HmacSHA512")` on the builder chooses another. There is deliberately no
  "authenticated" flag in the header, and nothing on the wire naming the algorithm, because an
  attacker would clear one and downgrade the other — both are local policy every member of a
  cluster has to share.
- **One thread per node.** A single NIO `Selector` loop is the only blocking point for both
  datagrams and timers.
- **Persist before acting.** `currentTerm` and `votedFor` are fsync'd before any message that
  depends on them is sent. A node that grants a vote and then forgets it can grant a second in the
  same term, and two candidates each collect a majority.
- **CheckQuorum and PreVote.** An isolated leader stands down instead of reporting `LEADER`
  forever; a rejoining node cannot force a failover nobody asked for. Both are mandatory here in a
  way they are not in full Raft, because with no log the only thing a leader produces is the answer
  to *am I the leader?*

## Testing

**Deterministic simulation is the primary harness**: N nodes on one thread over a virtual clock and
an in-memory transport that drops, delays, reorders and partitions on a seeded PRNG. Every scenario
runs under continuously-checked safety invariants — at most one leader per term, a node never
contradicting a vote it gave before a crash, a witness never becoming candidate or leader — and
**each invariant has a test proving it fires**, so a green suite means something.

Real sockets and real threads are a sanity check on top of that, not the safety net.

```
./mvnw verify
```

JDK 11 or newer. The library targets Java 11 (`maven.compiler.release`) and the test suite runs on
11 too, so the oldest supported JDK is exercised rather than merely compiled against.

## Modules

| module | what |
|---|---|
| `primos` | the library: API, role state machine, wire codec, transports, state store |
| `primos-witness` | the witness server and its config store, as a library with no `main` — [README](primos-witness/README.md), which is also the operator manual |
| `primos-micrometer` | meters for a node, and for a witness's served clusters — [README](primos-micrometer/README.md) |
| `primos-witness-cli` | the picocli command line and the `tar.gz`/`zip` distribution — [README](primos-witness-cli/README.md) |
| `primos-witness-boot` | the same server as a Spring Boot application, and the container image — [README](primos-witness-boot/README.md) |
| `primos-spring-boot-starter` | Spring Boot auto-configuration and a leadership-gated lifecycle — [README](primos-spring-boot-starter/README.md) |

The library and the witness are two artifacts rather than the usual api/impl split, deliberately:
module splits exist to isolate dependencies, and primos has none to isolate. The starter is the
exception that proves the rule — Spring Boot is exactly the transitive tree a consumer must be able
to decline, so it lives in its own artifact and the library references no Spring type. It is also
the only module not on Java 11, because Spring Boot's floor is 17.

## License

Apache License 2.0 — see [LICENSE.txt](LICENSE.txt).
