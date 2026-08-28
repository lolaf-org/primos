# Primos

Raft leader election for clustered Java applications. Every instance knows whether it is the
leader or a follower, and is told the moment that changes.

*Primus inter pares* — first among equals. Also *cousins* in Spanish and Portuguese: peers, one
of whom is currently first.

> **Status: design complete, no code yet.** The skeleton builds; the library is not written. Every
> design decision, and the reasoning behind it, is in [PLAN.md](PLAN.md).

## What it is

```java
PrimosNode node = PrimosNode.builder()
        .applicationName("orders")
        .nodeId(3)
        .peers(...)
        .witness(...)        // optional
        .transport(...)
        .store(...)
        .listener(new MyListener())
        .build();

node.start();

if (node.role() == Role.LEADER) { ... }
```

A single listener reports everything: this node becoming or ceasing to be leader, which node is
leader, and peers becoming reachable or unreachable.

## What it is not

**Leader election only.** No replicated log, no state machine, no snapshots, no runtime membership
changes. Your application keeps its own state; primos only tells it who is currently in charge.
There is no intention to extend it to replication.

## Design in one page

- **Zero dependencies.** `slf4j-api` at compile scope and nothing else — Lombok is compile-time
  only and not transitive. Plain JDK concurrency and NIO throughout.
- **UDP transport.** Raft already assumes a network that drops, delays, duplicates and reorders,
  and recovers from all of it with its own timers, so TCP's retransmission is duplicated work.
  Every message is fixed-size and fits one unfragmented datagram.
- **One thread per node.** A single NIO `Selector` loop is the only blocking point for both
  datagrams and timers.
- **Optional witness.** A two-node cluster tolerates zero failures. A leader-ineligible voter in a
  third failure domain makes it three voters with quorum two, so a partition has a winner and a
  loser rather than two winners. One witness server serves many clusters.
- **Safety extras that election-only scope makes mandatory**: `currentTerm`/`votedFor` fsync'd
  before any vote, CheckQuorum so an isolated leader steps down instead of reporting `LEADER`
  forever, and PreVote so a rejoining node cannot force a pointless failover.
- **Deterministic simulation is the primary test harness** — N nodes on one thread over a virtual
  clock and a transport that drops, delays, reorders and partitions on a seeded PRNG.

## Using it safely

There is always a window in which your application believes it is leader and is not — that is
inherent to leader election, not a defect. `role() == LEADER` is therefore **not** a sufficient
guard for an external side effect. Read the role and term together, pass the term to whatever
system the work touches, and have that system reject anything carrying a stale term.

## Modules

| module | what |
|---|---|
| `primos` | the library: API, role state machine, wire codec, transports, state store |
| `primos-witness` | the standalone witness server |

## Building

```
./mvnw verify
```

JDK 11 or newer. The library targets Java 11 (`maven.compiler.release`), and the test suite runs on
11 too, so the oldest supported JDK is exercised rather than merely compiled against.

## License

Apache License 2.0 — see [LICENSE.txt](LICENSE.txt).
