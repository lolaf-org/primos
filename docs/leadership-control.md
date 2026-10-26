# Planned switchover and maintenance

Leadership normally moves only when something goes wrong, or when the leader stops: [Clean shutdown](clean-shutdown.md)
hands it over on the way out. Two calls move it while every node keeps running.

## Hand leadership over without stopping

```java
if (node.transferLeadership()) {
    // told a follower to campaign; wait for onRoleChanged with a deadline of your own
}
```

`transferLeadership()` sends the same `TimeoutNow` a stopping leader sends, to the follower it most recently completed a
round trip with, and stays in the cluster. `transferLeadership(int targetId)` picks the follower instead.

- **The leader keeps leading until its successor campaigns.** The successor's vote request, in the next term, is what
  stands it down. A transfer whose message is lost, or whose target never campaigns, changes nothing: the cluster keeps
  its leader, and the caller sees no `onRoleChanged` before its own deadline.
- **The return value says whether a follower was told**, not whether leadership moved. It is false when the node is not
  leading or not running, when no follower was heard from within an election timeout, and for a target that is this
  node, not a configured peer, a witness, a peer that withdrew from elections, or one the probes declared down.
- **The outcome arrives as events**: `onRoleChanged` on this node and the successor, `onLeaderChanged` everywhere.

Use it to move leadership off a node before maintenance, or to bring it back to a preferred node, with the application
draining its own leader-only work first.

## Withdraw a node from elections

```java
node.setCandidate(false); // votes, never campaigns
node.setCandidate(true);  // stands again, after a full election timeout
```

A node that withdrew keeps voting, answering heartbeats and probing, so the quorum it belongs to still forms, but it never
campaigns and refuses a `TimeoutNow`: the witness's rules, switched on at runtime. Use it for maintenance, or while the
application is not fit to lead.

- **A leader that withdraws hands over first, and keeps leading until a successor takes over.** It offers leadership
  again every election timeout, so it never leaves the cluster without a leader by itself. If every other node withdrew
  too, it keeps leading and keeps offering.
- **Followers say so in every heartbeat reply**, so a leader never offers leadership to a node that withdrew.
- **A cluster whose every node withdraws has no leader.** That is the application's call; the
  `primos.candidate` gauge, summed across the cluster, is 0 when it happens.
- Kept across `stop()` and `start()` of the same node object, never persisted: a new node stands.

## Watching it

[Micrometer](micrometer.md): `primos.handover` with `direction=transferred` for each offer, `accepted` on the node that
took it, `primos.handover.no.successor` when a transfer found nobody standing, and `primos.candidate`.
[Observability](observability.md) has the matching `PrimosStatsListener` events.

## Mixed versions

A node that predates candidacy never sets the flag in its heartbeat replies, and reads as standing, which is what it
does. A leader that predates it may still offer leadership to a node that withdrew; that node refuses, and the leader
falls back to its election timeout as it would without the feature.
