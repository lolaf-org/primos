# Clean shutdown

A node that simply stops is *correct*: the cluster recovers on its own timers. But it is slow in two different ways,
and `stop()` closes both. Both are best-effort: one datagram, nothing waits for it, and if it is lost the cluster falls
back to exactly the behaviour it would have had anyway.

## A departing leader hands over

Stopping a leader would otherwise leave the cluster with nobody in charge until a follower's election timeout expires:
300 to 600ms on the LAN defaults, on every deploy and every restart. Instead it sends `TimeoutNow` (Raft thesis §3.10)
to the follower it has most recently completed a round trip with, telling it to campaign immediately. Failover becomes
about one round trip. The witness is never chosen, and would refuse anyway.

## Any departing node announces itself

Reachability is otherwise inferred from silence, and on the JVM silence has to be given a long benefit of the doubt. A
stop-the-world collection looks exactly like a dead process, so the probe timeout gets tuned up, often to tens of
seconds, and a node that shut down politely stays reported as alive for that whole window. Lowering the timeout brings
back the false positives the tuning existed to prevent.

So the fix is not a shorter timeout but a message. A shutdown is the one departure a node knows about in advance and
can say out loud: it sends `Leaving` to every peer, and `onPeerDown` fires at once instead of a timeout later, with
`PeerDownReason.LEFT` so a listener can tell an orderly stop from a peer that went silent (`UNREACHABLE`).

Crashes, kills, power loss and partitions still take the probe timeout, as they must. The timeout stays long; it just
stops being paid for the common case.

## Neither message changes the voter set

Quorum is always computed from configured membership and never from who happens to be reachable. Being told a peer is
leaving is a hint, and measurement stays authoritative: a peer that announces and then answers a probe anyway is
reported up again.

## Give it time

Both messages go out during `stop()`. Whatever supervises the process should send `SIGTERM` and wait, rather than
following up with `SIGKILL`: systemd's `TimeoutStopSec`, Docker's `--stop-timeout` and Kubernetes'
`terminationGracePeriodSeconds` all default to enough.

To move leadership without stopping the leader, or keep a node from leading at all, see
[Planned switchover and maintenance](leadership-control.md).

[Observability](observability.md) has the two counters that show this working: `onHandoverSent` against the deploys
that stopped a leader, and `onDepartureObserved` against the peer-down events.
