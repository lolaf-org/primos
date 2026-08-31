# Primos Spring Boot Starter

Add one dependency and a Spring Boot application becomes a member of a primos cluster: it knows
whether it is the leader, it is told the moment that changes, and — if you ask for it — Spring starts
and stops your beans as leadership comes and goes.

For the library, see the [project README](../README.md). For the reasoning behind every decision
here, [PLAN.md](../PLAN.md).

**Java 17 and Spring Boot 4.x**, alone among the modules — the library itself stays on Java 11. That
asymmetry is the reason this is a separate artifact: the constraint lives in the thing that accepts
it, not in the one every other consumer inherits.

---

## Quick start

```xml
<dependency>
    <groupId>org.lolaf.primos</groupId>
    <artifactId>primos-spring-boot-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```yaml
primos:
  application-name: orders          # nothing is auto-configured until this is set
  node-id: 1
  state-directory: /var/lib/orders/primos
  bind-port: 7420
  secret: ${PRIMOS_SECRET}
  peers:
    - { id: 2, host: orders-b, port: 7420 }
    - { id: 3, host: orders-c, port: 7420 }
```

```java
@Service
class Reconciler {

    private final PrimosNode node;

    Reconciler(PrimosNode node) {
        this.node = node;
    }

    void reconcile() {
        RoleAndTerm current = node.roleAndTerm();
        if (!current.isLeader()) {
            return;
        }
        externalSystem.write(work, current.term());   // the term is not optional — see below
    }
}
```

That is the whole surface. The node starts with the context, joins the cluster, and
`node.roleAndTerm()` answers at any moment.

**Nothing happens until `primos.application-name` is set.** The starter can sit on a classpath doing
nothing, which is what makes it safe to put in a shared parent pom.

---

## What it contributes

Every one of these backs off if your application defines its own bean of that type, so anything the
properties cannot express is an `@Bean` away.

| bean | notes |
|---|---|
| `Timings` | from `primos.profile`; define your own for anything other than `lan`/`wan` |
| `ClusterConfig` | application name, this node, its peers, any witness |
| `StateStore` | a `FileStateStore` on `primos.state-directory` |
| `Transport` | a `UdpTransport` on `primos.bind-address`/`bind-port` |
| `PrimosNode` | built, **not started** — see *When the node starts* |
| `PrimosNodeLifecycle` | starts the node (or not) and always stops it |
| `PrimosListener` | republishes primos' callbacks as Spring events |
| `LeaderLifecycleController` | only when `primos.leader-lifecycle.enabled=true` |
| `PrimosHealthIndicator` | only when actuator health is on the classpath |

Any `PrimosListener` bean your application defines is registered on the node before it starts, so it
cannot miss the first transition. **Do not inject `PrimosNode` into such a listener** — that is a
dependency cycle, because the node is built from the listeners. Use `@EventListener` instead, or
`ObjectProvider<PrimosNode>`.

---

## Configuration

| key | required | default | meaning |
|---|---|---|---|
| `primos.application-name` | **yes** | none | which cluster this is, and the switch that activates everything |
| `primos.node-id` | **yes** | `0` | this node's id, unique within the application |
| `primos.node-name` | no | `node-<id>` | a readable name for logs |
| `primos.state-directory` | **yes** | none | where the term and vote are fsynced |
| `primos.bind-address` | no | wildcard | local address to bind |
| `primos.bind-port` | in practice | `0` | local port. `0` takes an ephemeral one, which peers cannot be told about |
| `primos.peers[n].id/name/host/port` | in practice | none | the other application instances, static. None means a one-node cluster |
| `primos.witness.id/name/host/port` | no | none | the optional witness, at most one |
| `primos.secret` | strongly advised | none | the shared cluster secret |
| `primos.mac-algorithm` | no | `HmacSHA256` | any JCE `Mac` name; every member must agree |
| `primos.timings.profile` | no | `lan` | `lan` or `wan` — see *Timings* |
| `primos.timings.heartbeat-interval` | no | `50ms` | how often a leader asserts itself |
| `primos.timings.election-timeout-min` | no | `300ms` | low end of the randomised election timeout |
| `primos.timings.election-timeout-max` | no | `600ms` | high end; set with its pair |
| `primos.timings.probe-interval` | no | `1s` | how often every node probes every peer |
| `primos.timings.failure-threshold` | no | `3` | missed probes before a peer is reported down |
| `primos.vote-logging` | no | `true` | log who voted for whom at INFO instead of DEBUG |
| `primos.lifecycle` | no | `context` | `context` or `manual` — see below |
| `primos.leader-lifecycle.enabled` | no | `false` | whether leadership starts and stops beans |
| `primos.leader-lifecycle.scope` | no | `beans` | `beans` or `context` |
| `primos.leader-lifecycle.beans` | no | none | explicit bean names, winning over `@LeaderOnly` |

Only two keys fail fast when missing: `application-name` leaves everything unconfigured, and
`state-directory` refuses to start. The other three marked above fail *quietly*, which is worse:

- **`node-id` binds to `0` when unset**, and `0` is a valid id. A cluster whose nodes all forgot it
  is a cluster of node `0`s.
- **`bind-port: 0`** takes whatever ephemeral port the OS hands out, which no peer can be configured
  to reach. Fine for a test, never for a deployment.
- **No `peers`** is a one-voter cluster that elects itself and leads permanently — supported and
  useful, and also exactly what a peer list that failed to bind looks like. primos warns at startup
  when it sees it.

There is deliberately **no default for `state-directory`**. A default under a temp directory is
silently unsafe after a reboot, and a node that forgets its vote can elect a second leader.

**A node configured with no peers is a one-voter cluster and leads itself, permanently.** That is
supported and useful for development — and it is also what a peer list that failed to bind looks
like. primos warns at startup when it sees it; read the warning rather than filtering it.

---

## Timings

**Either a named profile or the intervals spelled out — never both.**

```yaml
primos:
  timings:
    profile: wan                    # a whole profile…
```

```yaml
primos:
  timings:                          # …or your own numbers
    heartbeat-interval: 250ms
    election-timeout-min: 2s
    election-timeout-max: 4s
    probe-interval: 3s
    failure-threshold: 5
```

Setting a profile *and* an individual interval is refused, with both named in the message. A profile
is a set of values picked so the relationships between them hold — Raft's `broadcastTime <<
electionTimeout << MTBF` — so "`wan` but with a different heartbeat" is not a profile any more, it is
a custom set whose author has not seen the other four numbers. Naming all five is a small price for
knowing what you configured.

You do not have to set all five. Anything omitted falls back to the default in the table above, and
**the relationships are validated for you**: set only `heartbeat-interval: 100ms` and startup fails
with *"electionTimeoutMin must be at least 5 heartbeat intervals (500ms) but was 300ms"*. The rules
are enforced in one place — `Timings` — rather than restated here.

**Those fallbacks are the LAN values, never the profile's** — a profile and an individual interval
cannot both be set, so when you are naming intervals there is no profile for them to fall back to.
The trap that follows is worth stating plainly: on a WAN, `probe-interval: 3s` on its own is a valid
configuration that silently keeps the 50ms heartbeat and the 300–600ms election timeout, and no
validation will object, because nothing about it is incoherent — only wrong for your network. **If
you are not on a LAN, name every value you care about**, not just the one you wanted to change.

The two election bounds are a **pair**: setting one without the other is refused. The timeout is a
range drawn from afresh on every reset, and setting one end against the other's default is how an
invalid or too-narrow range gets built. Randomisation is not decoration — nodes that time out
together campaign together, split the vote, and repeat.

**Keep it uniform across the cluster, witness included.** A node refuses a pre-vote while it has
heard from a leader within `electionTimeoutMin`, so a member with a longer one is proportionally
more reluctant to agree to the failover its vote is needed for. Nothing is corrupted by a mismatch;
failover just gets slower in a way that is very hard to see from either side. The witness server
takes the same five values — as integer milliseconds, `timings.heartbeat.millis` and friends — so a
custom profile can be matched there.

For anything these cannot express, define a `Timings` bean and the auto-configuration backs off.

---

## When the node starts — `primos.lifecycle`

Joining the cluster and being *ready to lead* are not the same event. Tying the first to context
refresh assumes they are, so which one happens is a setting.

### `context` (default)

The node starts as the context finishes refreshing.

```yaml
primos:
  lifecycle: context
```

Right for an application with nothing to do before it can safely lead. Nothing to write.

### `manual`

The starter builds a fully configured `PrimosNode` bean and does **not** start it. You start it when
the application is genuinely ready — after a cache warm, a schema migration, a first sync: the work
that must not be half-done in a process about to be told it is in charge.

```yaml
primos:
  lifecycle: manual
```

```java
@Component
class JoinTheClusterWhenReady implements ApplicationRunner {

    private final PrimosNode node;
    private final WarmCache cache;

    JoinTheClusterWhenReady(PrimosNode node, WarmCache cache) {
        this.node = node;
        this.cache = cache;
    }

    @Override
    public void run(ApplicationArguments args) {
        cache.warm();       // must not be half-done in a process that might be elected
        node.start();
    }
}
```

Without this, the alternative is to gate the *beans* on leadership while the node has already joined
and can win an election during boot — which puts the race one level down rather than removing it.

### Stopping is never manual, in either mode

Spring stops the node on context close whichever mode you choose. Only the start is ever yours.

That is not tidiness. A leader that stops cleanly sends `TimeoutNow` to a successor and `Leaving` to
everyone, so a deploy costs about **one round trip** of leaderlessness instead of a full election
timeout, and peers notice the departure at once rather than after a GC-tolerant probe timeout.
Leaving that to an application that has to remember to call `stop()` would trade away the most
valuable thing primos does at shutdown for a symmetry nobody asked for.

You may still call `stop()` yourself if you want the node gone earlier. `stop()` is idempotent, so
the context close is then a no-op.

### The one silent failure `manual` introduces

Configured, never started, with gating enabled: the gated beans wait for leadership that cannot
arrive. No election, no error, and nothing in any log — the application simply does nothing, for
ever.

So the starter warns at `ApplicationReadyEvent` (runners have already run by then, so a node started
in one is started by now):

```
cluster 'orders': node 1 has not been started, and leader-gated beans are waiting on leadership
that cannot arrive until it is. Either call PrimosNode.start(), or set primos.lifecycle=context to
have Spring start it.
```

and health reports `"state": "not-started"` rather than letting it read as an ordinary follower.

---

## Starting and stopping beans on leadership — `primos.leader-lifecycle`

Off by default. When on, Spring `Lifecycle` beans are started as this node becomes leader and
stopped as it loses leadership.

```yaml
primos:
  leader-lifecycle:
    enabled: true
```

Which beans is a setting, and there are three ways to say it.

### 1. `@LeaderOnly` — the default

Mark any `Lifecycle` or `SmartLifecycle` bean:

```java
@Component
@LeaderOnly
class OutboxPublisher implements SmartLifecycle {

    private volatile boolean running;

    @Override
    public void start() {
        running = true;
        poller.begin();
    }

    @Override
    public void stop() {
        poller.drainAndHalt();      // may take a while; it is not on the node thread
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
```

Nothing else to configure. A bean annotated `@LeaderOnly` that is *not* a `Lifecycle` is warned about
and skipped, because there would be nothing to start or stop.

**The annotation does nothing on its own.** It is read only when
`primos.leader-lifecycle.enabled=true`; without that there is no scan, no gating and no warning.

### 2. An explicit bean list — wins over the annotation

```yaml
primos:
  leader-lifecycle:
    enabled: true
    beans: outboxPublisher,scheduledReconciler
```

For gating beans you do not own, or overriding the annotations without editing them. When this list
is set, `@LeaderOnly` is ignored entirely — one source of truth, not two merged. A name that does not
exist fails the context at startup, with the name in the message.

### 3. `scope: context` — every `Lifecycle` bean

```yaml
primos:
  leader-lifecycle:
    enabled: true
    scope: context
```

For an application whose entire reason to exist is the leader work.

**Read this before choosing it.** It gates *every* `Lifecycle` bean in the context, the web server
and the actuator among them — so losing leadership stops this application answering health checks at
exactly the moment an operator or a load balancer wants to read one. A failover that blinds the
monitoring is worse than the one it replaced. Selecting it logs a warning saying so.

primos' own beans and Spring's `LifecycleProcessor` are always excluded, whatever the scope. Gating
the node on leadership would stop the very thing that reports it.

### What it guarantees

**Gated beans are never left running at boot.** A `SmartLifecycle` that auto-starts is running before
the first election has concluded — a follower would be consuming from its queues within a second of
boot with nothing anywhere to say so. The starter stops whatever auto-started at
`ContextRefreshedEvent`, *before* the node can be told it leads, in both lifecycle modes.

> A `SmartLifecycle` that does not override `getPhase()` sits at `Integer.MAX_VALUE`, so no phase can
> be ordered after one. Refresh is the only hook genuinely after every auto-start, which is why the
> quiescing happens there rather than through phases.

**Transitions never run on the node thread.** They run on a dedicated single-threaded executor
(`primos-leader-lifecycle`). Stopping a message listener container drains it and can take tens of
seconds; on the node thread that would stall consensus, miss heartbeats and trigger a spurious
election — which loses leadership, which triggers another stop. Being single-threaded is also what
stops a start and a stop from interleaving.

**Stale transitions are dropped by term.** Leadership can be lost and regained faster than a stop
completes. A stop from term 5 arriving after the start for term 6 is ignored, because applying it
would leave a leader with everything switched off — silent, indefinite, and the worst failure this
can have.

**A bean that throws does not take the others with it**, and never reaches the node.

---

## Leadership as Spring events

```java
@Component
class LeadershipListener {

    @EventListener
    void onRole(PrimosEvents.RoleChanged event) {
        if (event.isLeader()) {
            log.info("we lead in term {}", event.term());
        }
    }

    @EventListener
    void onLeader(PrimosEvents.LeaderChanged event) {
        event.leader().ifPresent(leader -> {
            if (!leader.isSelf()) {
                leader.peer().ifPresent(peer -> forwardWorkTo(peer.host(), peer.port()));
            }
        });
    }

    @EventListener
    void onPeerDown(PrimosEvents.PeerDown event) {
        // Reachability, not membership. The cluster did not shrink and quorum is unchanged.
        alerting.warn("peer {} is unreachable", event.peer().id());
    }
}
```

Four events: `RoleChanged`, `LeaderChanged`, `PeerUp`, `PeerDown`.

**Every event carries its own term. Use the value you are handed** rather than re-reading the node:
dispatch happens off the node thread, so by the time your listener runs the answer may have moved
on. That is not a race to be fixed — it is why the term is on the event.

`LeaderChanged` carries a `Leader`, which distinguishes three states a bare id could not: this node
leads (`isSelf()`, no peer to forward to), a peer leads (`peer()` present — this is where work
goes), and a leader is known but not yet identified. Note that the nodes of a healthy cluster agree
on *who* leads but not on the object saying so — the leader reports `isSelf()`, everyone else reports
a peer. Compare `id()`.

---

## Health

With actuator on the classpath, `/actuator/health` reports:

```json
{
  "status": "UP",
  "details": {
    "application": "orders",
    "nodeId": 1,
    "state": "running",
    "role": "FOLLOWER",
    "term": 7,
    "leader": "3"
  }
}
```

**A healthy follower is `UP`.** Only `FAILED` is `DOWN`. Getting that backwards takes every follower
out of a load balancer during an ordinary election — a whole-cluster outage caused by the health
check rather than detected by it.

`FAILED` means the node thread died and this node will never lead again; restart the process rather
than waiting for it to recover. `"state": "not-started"` stays `UP` — under `manual` it is the
expected state until you start the node — but it says so, because a deployment that forgot to start
its node looks identical to a healthy one from every other angle.

---

## Using it safely

**This module makes leadership convenient. It does not make it safe, and it is the thing most likely
to be mistaken for something that does.**

There is always a window in which this node believes it leads and does not. Leadership can be lost at
any instant and the news travels no faster than the network. Between losing leadership and a gated
bean's `stop()` completing, in-flight work is still running — and a `Lifecycle` stop cannot be
fenced.

So `@LeaderOnly` is not a guard. The guard is the term:

```java
RoleAndTerm current = node.roleAndTerm();   // role and term, read together
if (current.isLeader()) {
    externalSystem.write(work, current.term());
}
```

and the external system rejects anything carrying a term lower than the highest it has seen. Anything
guarded only by a boolean will eventually be wrong.

---

## What this module does not do

It packages, configures and observes the election that already exists. It adds no message to the
wire, no field to any header, and no decision a node does not already make. There is no replicated
log, no state machine and no runtime membership change — your application keeps its own state, and
primos only tells it who is currently in charge.
