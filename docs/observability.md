# Observability

`slf4j-api` is the only dependency, so there is no metrics registry in the library. The node tells you what it did, as
it does it, and you increment whatever you already run:

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

If you run Micrometer, [`primos-micrometer`](micrometer.md) is exactly such a listener, already written.

What you branch on is still a plain call: `role()`, `roleAndTerm()`, `leader()`, `isRunning()`.

## The contract

**Callbacks must not block, and must not allocate.** They run on the node thread and some of them run per datagram, so
every method takes primitives and enums, there is one sink rather than a list, and there is no executor: an iterator or
a `Runnable` per drop is the allocation the shape exists to avoid. Compose several with `PrimosStatsListener.of(a, b)`.

A listener that throws is caught and logged: a metrics binder must not be able to stop consensus. A test asserts that a
counting listener adds **zero** bytes a turn on the drop path, because otherwise the rule is only a comment.

## Why there is no `stats()` snapshot

A snapshot cannot carry the three things worth having:

- a **distribution**, since a p99 needs every sample and no polling interval recovers one from a last-value gauge;
- a **duration that begins and ends between two samples**, like how long a campaign took or how long the cluster had
  no leader, the two numbers most wanted after a failover;
- an **event already superseded**, like leadership going A to B to A inside one interval, which is invisible to a
  poller and is exactly the flap worth alerting on.

## What gets reported

| callback | what it says |
|---|---|
| `onElectionWon` / `onElectionLost` | with how long the campaign took, timed from the pre-vote |
| `onLeaderKnown` | with how long the cluster had none, across every election in the outage |
| `onLeadershipRelinquished` | CheckQuorum stood this node down: it could not reach a majority |
| `onHandoverSent` / `onHandoverAccepted` | leadership handed on at shutdown, and taken up |
| `onDepartureObserved` | a peer said it was going |
| `onProbeRoundTrip` | every sample, per peer |
| `onDatagramDropped` | with the reason; see the [receive pipeline](protocol.md#the-receive-pipeline) |
| `onTimingMismatch`, `onUnusableNodeConfig`, `onPeerForgotten` | configuration a peer stated |

Two answer questions a rolling deploy raises immediately. `onHandoverSent` against the number of times this node was
stopped while leading is the count of deploys that cost the cluster an election timeout. `onDepartureObserved` against
the peer-down events separates orderly shutdowns from departures merely inferred from silence, which on the JVM might
have been a garbage collection.

Role, leader and peer up/down are **not** here: they are on `PrimosListener`, and a binder that wants both implements
both. Two ways to hear one thing is how two dashboards come to disagree.

The witness server registers one listener per served application, so a stray cluster on a witness serving whatever it
hears is visible on a dashboard.
