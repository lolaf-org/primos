# Wire protocol

Primos follows the leader-election half of [Raft](https://raft.github.io/raft.pdf) closely: the same terms, the same
`RequestVote` and `AppendEntries` exchanges, the same one-vote-per-term rule, plus the PreVote, CheckQuorum and
leadership-transfer extensions from Diego Ongaro's thesis. What it drops is everything that exists for the replicated
log, and what it adds is reachability probing, a departure notice and a configuration statement.

It speaks that protocol over UDP: nine message types, one fixed-size datagram each. This page describes every byte of
it, for anyone writing a dissector, reading a capture, or checking the codec against the Raft paper.

The codec lives in `org.lolaf.primos.wire`, which is internal: its classes are public only so the witness, a separate
artifact, can use them. The format is versioned, and this page describes version 1.

## Why UDP, and why fixed sizes

Raft already assumes a network that drops, delays, duplicates and reorders messages, and recovers from all of it with
its own timers. TCP's retransmission would be duplicated work, and its head-of-line blocking would delay the heartbeat
that matters behind one that no longer does.

**Every message type has a size known at compile time.** One length comparison rejects every truncated, padded or
malformed datagram before a field is read, and the parser contains no loops. There is no length prefix and no framing,
because UDP gives the frame: a datagram arrives whole or not at all.

The largest message, `NodeConfig` with an HMAC-SHA256 tag, is 104 bytes. Primos never sends or accepts a datagram over
508 bytes, the largest UDP payload every IPv4 host must accept without fragmentation, so there is no reassembly
anywhere.

## The header

Every message starts with the same 52 bytes. Integers are big-endian (network byte order) and unsigned unless noted.

| offset | size | field | notes |
|-------:|-----:|-------|-------|
| 0 | 4 | magic | ASCII `PRMS` |
| 4 | 1 | version | `1`. An unknown version is dropped, never guessed |
| 5 | 1 | message type | see the table below. Never `0`, so a zeroed buffer is not a message |
| 6 | 2 | flags | bit 0: pre-vote. Other bits are reserved, sent as zero and ignored on receive |
| 8 | 32 | application name | UTF-8, zero-padded. 1 to 32 bytes of printable ASCII, no spaces |
| 40 | 8 | term | signed 64-bit, the sender's current term (with one exception, below) |
| 48 | 4 | sender id | signed 32-bit, the sender's node id |
| 52 | | payload | per message type, possibly empty |
| | | tag | when authentication is on: the HMAC over everything before it |

**The application name travels readable rather than hashed**, so a drop can say which foreign cluster is talking:
"expected `orders`, got `ordres`" diagnoses a typo in one line. It is also what lets one witness serve many clusters on
one port.

**There is no candidate id and no leader id.** Raft's `RequestVote` names its candidate and `AppendEntries` names its
leader; here that field simply is the sender, already in the header.

## Messages

| code | message | payload | total, untagged | purpose |
|-----:|---------|--------:|----------------:|---------|
| `0x01` | `RequestVote` | 0 | 52 | ask for a vote, or with the pre-vote flag, whether one would be given |
| `0x02` | `RequestVoteReply` | 1 | 53 | grant or refuse it |
| `0x03` | `AppendEntries` | 0 | 52 | the leader's heartbeat |
| `0x04` | `AppendEntriesReply` | 1 | 53 | acknowledge the heartbeat |
| `0x05` | `Ping` | 8 | 60 | reachability probe |
| `0x06` | `PingReply` | 8 | 60 | probe answer |
| `0x07` | `TimeoutNow` | 0 | 52 | a departing leader telling one follower to campaign now |
| `0x08` | `Leaving` | 0 | 52 | a node announcing its own shutdown |
| `0x09` | `NodeConfig` | 20 | 72 | a node stating the intervals it runs |

Add the tag length to each total when authentication is on: 32 bytes for the default HMAC-SHA256.

### `RequestVote` (`0x01`)

No payload. The term is the one the candidate proposes, and the sender is the candidate.

With the **pre-vote flag** set, it asks "would you vote for me in this term?" and the term is the candidate's current
term plus one. A pre-vote changes nothing on the receiver: no term adopted, no vote recorded, no timer reset. That is
what lets a node that was partitioned away find out it cannot win without forcing the healthy cluster through an
election. A node refuses a pre-vote while it has heard from a leader within its own minimum election timeout.

Without the flag it is Raft's ordinary vote request. **Departure from the paper:** there is no `lastLogIndex` or
`lastLogTerm`, because there is no log. Every voter is equally up to date, so the only rule is one vote per term.

### `RequestVoteReply` (`0x02`)

| offset | size | field |
|-------:|-----:|-------|
| 52 | 1 | granted: `1` or `0` |

Carries the pre-vote flag of the request it answers, so the two rounds cannot be confused.

The term differs between the two rounds. A **pre-vote reply echoes the term that was proposed**, so a reply from an
abandoned round cannot be counted in the current one; this is the one place the header term is not the sender's own. A
**vote reply carries the voter's current term**, as in the paper, so a candidate that is behind learns it and steps
down.

A voter records `votedFor` durably before sending a granted reply. Votes are counted in a set by sender id, so a
duplicated datagram is never a second vote.

### `AppendEntries` (`0x03`)

No payload. It never carries entries, because there is no log: it is purely the heartbeat by which a leader asserts its
term. A receiver whose term is not ahead becomes a follower of the sender and resets its election timer.

### `AppendEntriesReply` (`0x04`)

| offset | size | field |
|-------:|-----:|-------|
| 52 | 1 | success: `1` if the heartbeat was accepted, `0` if its term was stale |

The term is the responder's current term. A reply to a heartbeat in the leader's current term is the **only** thing
that proves a round trip to that peer, and CheckQuorum decides from those round trips whether the leader may keep
leading.

### `Ping` (`0x05`) and `PingReply` (`0x06`)

| offset | size | field |
|-------:|-----:|-------|
| 52 | 8 | sequence number |

Every node probes every peer every probe interval, whatever its role, so reachability (`onPeerUp`, `onPeerDown`) and
round-trip times are known on followers too. The reply echoes the sequence number, which is how the round trip is
measured and how a reply to a probe already given up on is told apart and ignored.

Probes deliberately do not count as contact for CheckQuorum: only a heartbeat reply does. A probe answer proves a peer
is alive, not that it accepts this node as leader.

### `TimeoutNow` (`0x07`)

No payload. Sent by a leader that is stopping, to the follower it most recently completed a round trip with, telling it
to campaign immediately instead of waiting out an election timeout (Raft thesis §3.10). The term is the leader's, so a
follower that has already moved past it ignores the instruction. A witness always refuses it, whatever the sender.

### `Leaving` (`0x08`)

No payload. Sent by any node that is stopping, to every peer, so they report it down at once rather than after a probe
timeout. **It is a reachability hint, never a membership change**: the voter set and the quorum are unchanged, and a
peer that sends `Leaving` and then answers a probe is reported up again.

### `NodeConfig` (`0x09`)

| offset | size | field |
|-------:|-----:|-------|
| 52 | 4 | heartbeat interval, milliseconds |
| 56 | 4 | election timeout minimum, milliseconds |
| 60 | 4 | election timeout maximum, milliseconds |
| 64 | 4 | probe interval, milliseconds |
| 68 | 4 | failure threshold, missed probes |

Each value is a signed 32-bit integer. A node restates its configuration every probe interval while any peer has not
answered it, then every advertisement interval (30s by default) once all have.

**Between nodes it is purely diagnostic.** A receiver compares the values with its own and reports a mismatch, and
changes no term, role or deadline whatever it finds. Values rather than a hash, so the log can say "peer 2 runs a 200ms
heartbeat, this node 50ms".

A witness server is the one exception. Under `config.source=cluster` it follows the intervals its clusters state, and a
`NodeConfig` is the only message that may create an application on it; see the [witness manual](witness.md).

## Authentication

With a cluster secret set, every datagram ends with a MAC computed over all the bytes before it, header and payload.
HMAC-SHA256 by default, any JCE `Mac` by configuration; tags shorter than 128 bits are refused.

**Nothing on the wire says whether a datagram is authenticated, or with which algorithm.** A flag would let an attacker
clear it and strip authentication; an algorithm field would let one pick the weakest algorithm both ends accept. Both
are local policy every member of a cluster shares, and the exact-size check enforces it: an untagged datagram is the
wrong size for a node expecting a tag. The consequence for an operator is that a wrong algorithm looks exactly like a
wrong key.

It authenticates and does not encrypt, and it does not stop a captured datagram being replayed. Replay is absorbed by
the protocol instead: an old-term replay fails the term check, a current-term replay is a duplicate UDP already forces
the protocol to tolerate, and a replayed vote grant is a no-op because votes are counted in a set.

## The receive pipeline

Every datagram goes through the same checks in the same order, and each failure is counted under its own
`DropReason`:

| step | check | dropped as |
|-----:|-------|-----------|
| 1 | at least a header, at most 508 bytes | `MALFORMED` |
| 2 | the tag verifies | `BAD_TAG` |
| 3 | magic and version | `BAD_MAGIC`, `UNKNOWN_VERSION` |
| 4 | application name is this cluster's | `WRONG_APPLICATION` |
| 5 | known type, exact size for it | `UNKNOWN_TYPE` |
| 6 | sender is a configured peer, by id | `UNKNOWN_PEER` |
| 7 | parse at constant offsets and dispatch | |

**Verify before parse.** Nothing is read before the tag is checked. The witness server has two documented exceptions,
both reading a fixed-offset field before the key is known: the application name, because it chooses which key to try,
and the message type, because only a `NodeConfig` may create an application. Neither grants a vote or moves a term on
its own; the datagram still runs the full pipeline afterwards.

Drops are logged at DEBUG only. Logging every dropped datagram at a visible level would turn a flood into a second
denial of service; the counter is the signal.

## Term rules

Every handler applies Raft's universal rule first: a message carrying a term higher than the receiver's makes it adopt
that term and step down to follower. A new term is persisted before anything that depends on it is sent. A message with
a lower term is ignored, except that a stale heartbeat or vote request is answered with the receiver's current term so
its sender learns it is behind.
