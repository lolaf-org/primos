# Using leadership safely

**This is the page most likely to prevent an incident.**

There is always a window in which your application believes it is leader and is not. That is inherent to leader
election, not a defect here: leadership can be lost at any instant, and the news travels no faster than the network.
CheckQuorum bounds the window; nothing can remove it.

## Carry the term

`role() == LEADER` is **not** a sufficient guard for an external side effect. Carry the term with the work:

```java
RoleAndTerm current = node.roleAndTerm();   // read together, never separately
if (current.isLeader()) {
    database.write(record, current.term()); // and have the database reject a stale term
}
```

The external system rejects anything carrying a term lower than the highest it has seen. That is what makes a stale
leader's write *fail* rather than corrupt. Anything guarded only by a boolean will eventually be wrong.

Terms only ever increase, and at most one node leads in any term, so the term is a fencing token: a write carrying term
7 after one carrying term 8 can only come from a leader that has already been replaced.

The same holds for the [Spring Boot starter](spring-boot.md#using-it-safely): `@LeaderOnly` makes leadership
convenient, not safe, because a bean's `stop()` cannot be fenced either.

## Do not block the node thread

Listener callbacks arrive on the node thread, the same thread that sends heartbeats and answers votes. Hand real work to
your own executor. A callback slower than half the heartbeat interval is logged, because a slow enough one makes this
node miss its own heartbeats and triggers the failover it was trying to react to.

## Treat `FAILED` as a restart signal

A node whose store can no longer make a vote durable becomes `FAILED` and stops claiming leadership permanently,
because continuing would mean voting on state it may forget. It cannot be restarted in place. Restart the process.

## Never lose the state directory

The term and the vote are fsynced before any message depending on them is sent. A node that grants a vote and then
forgets it can grant a second in the same term, and two candidates each collect a majority. So the state directory must
be on durable storage that survives a restart, and must never be wiped or restored from an old copy.
