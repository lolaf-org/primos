# Timings

Two profiles ship. `Timings.lan()` is the default; `Timings.wan()` is for a cluster whose witness sits on another
network. The witness server takes the same choice as `timings.profile = lan | wan`.

| parameter | `lan()` | `wan()` | what it controls |
|---|---|---|---|
| heartbeat interval | 50ms | 200ms | how often a leader asserts itself |
| election timeout | 300 to 600ms | 1.5 to 3s | how long silence is tolerated before campaigning |
| probe interval | 1s | 2s | how often every node pings every peer |
| failure threshold | 3 | 3 | missed probes before `onPeerDown` |

## Choosing a profile

**Choose by the round trip to the furthest voter.** That is the only question that matters, and for a witness
deployment the furthest voter is almost always the witness. Raft's governing inequality is
`broadcastTime << electionTimeout << MTBF`, with the election timeout an order of magnitude above the round trip. A
50ms WAN round trip against a 300ms election timeout is only 6x, which is why `lan()` is marginal the moment a voter is
off-site.

**Use one profile for the whole cluster, witness included.** A node refuses to grant a *pre-vote* while it has heard
from a leader within `electionTimeoutMin`, so a witness on `wan()` inside an otherwise `lan()` cluster is five times
more reluctant to agree to a failover than its peers, and that is exactly the vote a two-node pair is waiting on. Mixed
profiles do not corrupt anything; they make failover slower in a way that is very hard to see.

## What the numbers cost you

**Failover after a crash** is roughly `electionTimeoutMax + 2 x RTT`: the randomised timeout has to expire, then a
pre-vote round, then the election. About **0.6s** on `lan()`, about **3.1s** on `wan()`. Halving it means halving the
election timeout, which buys spurious elections; there is no free direction here.

**Failover after a clean stop** is about **one round trip**, because the departing leader hands over rather than going
silent: single-digit milliseconds on a LAN. Planned restarts do not pay the timeout above; only genuine crashes do. See
[Clean shutdown](clean-shutdown.md).

**A peer is reported down** after `probeInterval x failureThreshold`, plus up to one interval before the first
unanswered probe: **3 to 4s** on `lan()`, **6 to 8s** on `wan()`. A node that shuts down cleanly is reported at once
instead, because it says so.

## The JVM caveat, which is the one that actually bites

**A stop-the-world pause is indistinguishable from a dead process.** If the leader stalls for longer than a follower's
election timeout, the follower campaigns: a failover caused by garbage collection, not by any failure. The `lan()`
defaults assume worst-case pauses comfortably under 300ms, which is realistic for a modestly sized heap on G1, or for
ZGC or Shenandoah at almost any size, and *not* realistic for a large heap that can still reach a full GC.

So measure your worst-case pause rather than your average, and keep `electionTimeoutMin` well above it; 3x is a
reasonable starting point. Setting a 30s election timeout to be safe is a legitimate choice on a machine with 30s
pauses. It just means 30s failovers, and that trade should be made deliberately rather than discovered.

The same reasoning applies to the probe interval and `failureThreshold`. That product is why a GC-tolerant deployment
might set 10s probes, and why departures are *announced* rather than left to be inferred from silence.

**Storage latency is the other floor.** A vote is fsynced before it is answered, so a store whose worst-case sync
approaches the election timeout will lose elections while writing. Local SSD is tens to hundreds of microseconds and
irrelevant here; a network filesystem or a throttled cloud volume is neither. There is no warning for this yet.

## Deriving your own

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
2. Set `electionTimeoutMin` to comfortably exceed both: an order of magnitude over the round trip, several times the
   pause.
3. Set the heartbeat to at most a fifth of that.
4. Set `electionTimeoutMax` to roughly twice the minimum. The spread is not decoration: randomisation per node per
   election is a hard requirement from Raft §5.2, because nodes that time out together campaign together, split the
   vote, and repeat.
5. Set the probe interval by how quickly you want `onPeerDown`, bounded by GC tolerance.

`build()` rejects what would thrash, rather than letting it be found in production: an election timeout under 5x the
heartbeat, a randomisation range narrower than one heartbeat, a probe interval over 15s (a NAT pinhole commonly
expires at 30s, and a hole closing between probes reads as a peer flapping), a `failureThreshold` below 1, and any
non-positive duration.

## Mismatched timings are reported

Every node restates its intervals to its peers in a [`NodeConfig`](protocol.md#nodeconfig-0x09) message, and a peer
whose intervals differ is logged at ERROR with both sets side by side, and reported to `onTimingMismatch`. Coming back
into agreement is logged too, because during a rolling change half a cluster disagrees by design and you need to see it
end.

The cadence follows reachability rather than a fixed clock: **while any peer has not answered, a node restates itself
every probe interval; once they all have, every advertisement interval** (30s by default). One statement then thirty
seconds of silence is the right steady state and the wrong bootstrap: this is UDP, and a lost first statement is
exactly the case that most needs another.

**Between nodes it reports and never acts.** A mismatch makes failover slower, never unsafe, so nothing about the
cluster's behaviour changes when one is found. A node that refused a vote over a mismatch would turn a rolling timing
change into a partition, which is worse than the problem.

A witness server is the one exception, and only when its operator asks for it: under `config.source=cluster` it
*follows* the intervals its clusters state instead of reporting them, which lets a tenant retune without anybody
editing the witness. The argument above is about nodes chasing each other during a rolling change, and a witness is not
a member of that conversation. See the [witness manual](witness.md#where-configuration-comes-from-configsource).
