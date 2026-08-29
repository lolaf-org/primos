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

UdpTransport transport = UdpTransport.builder()   // throws IOException
        .bindAddress(new InetSocketAddress(7420))
        .peers(peers)
        .addressLearning(true)                    // only ever with a secret set
        .build();

ClusterConfig cluster = ClusterConfig.builder()   // who is in the cluster
        .applicationName("orders")
        .nodeId(1)
        .peers(peers)
        .build();

PrimosNode node = PrimosNode.builder()            // and how this node runs
        .clusterConfig(cluster)
        .clusterSecret(secret)
        .store(new FileStateStore(Paths.get("/var/lib/orders/primos")))
        .transport(transport)
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
| 2 nodes | 2 | 2 | **0** |
| 2 nodes + witness | 3 | 2 | 1 |
| 3 nodes | 3 | 2 | 1 |
| 3 nodes + witness | 4 | 3 | **1** |

A witness helps even-sized clusters and hurts odd-sized ones, so primos **warns and continues**
rather than refusing — every node logs a quorum summary at startup, plus a warning when the
configuration is strictly dominated by an alternative.

### The witness server

`primos-witness` is a standalone process serving many clusters at once, keyed by application name.
**Operators want [its own README](primos-witness/README.md)** — installing, configuring, secret
rotation, registration modes, what to alert on, and troubleshooting. In brief, it ships as a
`tar.gz`/`zip` with a launcher, its jars, and a commented sample configuration:

```
bin/primos-witness --config /etc/primos-witness
```

```
/etc/primos-witness/witness.properties            # port, mode, state directory, default secret
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

Secrets are per-application with a witness-wide fallback, and accept a **previous** secret
alongside the current one, so rotation is two file edits with a window rather than a flag day
across every node.

**Registration mode defaults to `strict`**, and here is why. In `permissive` mode the witness
serves any name it hears. Suppose two clusters are configured `orders` and `ordres`. They drop
each other's datagrams for a mismatched name, so each sees the other as unreachable and asks the
witness instead. The witness auto-creates *two* buckets and grants a vote from each — **two leaders
out of one typo**, with the cross-talk protection hiding the cause. Permissive mode is a legitimate
choice and is hardened for it (WARN on every auto-registration, near-miss name detection, and a cap
so an open witness cannot be filled up), but strict is the default for that reason.

## Observability

`slf4j-api` is the only dependency, so there is no Micrometer and no metrics registry. Instead
`node.stats()` returns an immutable snapshot to wire into whatever you already use:

```java
PrimosStats stats = node.stats();
stats.role();                       // and currentTerm(), timeInRole()
stats.electionsStarted();           // and electionsWon()
stats.handoversSent();              // leaderships handed on at shutdown
stats.handoversAccepted();          // times this node was told to campaign
stats.departuresObserved();         // peers that said they were leaving
stats.drops(DropReason.BAD_TAG);    // and every other reason, plus totalDrops()
stats.peers();                      // per peer: reachable, lastRoundTrip, sinceLastHeard
```

Two of those answer questions a rolling deploy raises immediately. `handoversSent` against the
number of times this node was stopped while leading is the count of deploys that cost the cluster
an election timeout. `departuresObserved` against the peer-down events separates orderly shutdowns
from departures merely inferred from silence — which on the JVM might have been a garbage
collection.

The witness server exposes the same shape per served application, so a stray cluster on a
permissive witness is visible on a dashboard.

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
        .electionTimeout(Duration.ofMillis(1_500), Duration.ofMillis(3_000))
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
  Eight message types, all fixed-size, the largest 92 bytes including its authentication tag —
  every one fits an unfragmented datagram.
- **Optional HMAC-SHA256 on every datagram**, verified before anything is parsed. There is
  deliberately no "authenticated" flag in the header, because an attacker would clear it.
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
| `primos-witness` | the standalone witness server, its config store, and its distribution |

Two artifacts rather than the usual api/impl split, deliberately: module splits exist to isolate
dependencies, and primos has none to isolate.

## License

Apache License 2.0 — see [LICENSE.txt](LICENSE.txt).
