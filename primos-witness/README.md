# Primos Witness Server

A **witness** is a voter that never becomes leader. This is the standalone process that provides
one to many clusters at once.

Operator-facing. For the library, see the [project README](../README.md); for the reasoning behind
every decision here, [PLAN.md](../PLAN.md).

---

## Why you would run one

Two nodes alone form a 2-voter cluster with quorum 2, which tolerates **zero** failures. Any death
or partition leaves no leader at all — an active/passive pair looks redundant and is not.

A witness in a **third failure domain** makes it 3 voters with quorum 2. Whichever node still
reaches the witness has a majority and leads; the other stands down. A partition gets a winner and
a loser instead of two winners.

| configuration | voters | quorum | tolerates |
|---|---|---|---|
| 2 nodes | 2 | 2 | **0** |
| 2 nodes + witness | 3 | 2 | 1 |
| 3 nodes | 3 | 2 | 1 |
| 3 nodes + witness | 4 | 3 | **1** |

It helps even-sized clusters and hurts odd-sized ones — 3 nodes + witness needs three of four
processes alive instead of two of three. That is suboptimal, not unsafe, so primos warns and keeps
going. Every cluster logs a quorum summary at startup.

**"Third failure domain" is the whole point.** A witness on the same host, rack, power feed or
availability zone as one of the nodes buys you very little: the failure that takes out that domain
takes out the majority with it.

---

## Three rules that are safety, not advice

Break any of these and the witness causes the split brain it was deployed to prevent.

### 1. Never run two instances for one identity

No virtual address, no load balancer, no DNS round-robin, no "let's run two for redundancy". Two
witnesses answering for one cluster is **a double vote by construction** — each grants its own vote
in the same term and two nodes each believe they have a majority.

If you need the witness itself to be highly available, the answer is that you do not: losing it
returns the cluster to the fault tolerance it had without one, which is the state you were in
before you deployed it. That is a degradation, not an outage.

### 2. Never wipe the state directory

Not an ephemeral container volume, not a `tmpfs`, not "clear it and restart". The witness fsyncs
`currentTerm`/`votedFor` per application **before** answering any vote. A witness that forgets a
vote and restarts can vote twice in one term and produce two leaders.

It refuses to start on a store that reports itself non-durable, rather than warning.

Removing an application's configuration stops it being served but **leaves its state on disk**,
deliberately: a cluster that comes back later still needs the vote it was given remembered. A
permissive witness therefore accumulates state until an operator cleans it up.

### 3. Never run without a secret

Authentication protects **replies** as much as requests. One forged vote grant, plus the
candidate's own vote, is a majority in a three-voter cluster — so an unauthenticated witness link
is a direct route to a false leader, not merely a denial-of-service risk.

It is also the only admission control the witness has. A node checks that a sender is one of its
configured peers; the witness cannot, because it learns its voters from traffic. **The tag is the
whole of the gate.**

Without a secret the witness is also a small reflector: spoof a source address and it replies
there. It is not an amplifier — every reply is smaller than its request — and with a secret set it
cannot be provoked at all.

---

## Installing

Unpack the distribution wherever you keep such things:

```
primos-witness-<version>/
├── bin/primos-witness
├── lib/*.jar
├── conf-sample/                 # heavily commented; copy it, do not point at it
│   ├── witness.properties
│   └── applications/example.properties
└── LICENSE.txt
```

```sh
install -d -m 0750 /etc/primos-witness/applications
install -d -m 0750 /var/lib/primos-witness
cp -r primos-witness-*/conf-sample/* /etc/primos-witness/
```

`conf-sample/` is deliberately a *sample* rather than the live path, so that upgrading never
overwrites `witness.properties` — an upgrade that replaced your secrets with placeholders would
take out every cluster the witness serves.

Requires a JRE 11 or newer on `PATH`, or `JAVA_HOME` set.

### Running

```sh
bin/primos-witness --config /etc/primos-witness
```

| option | default | notes |
|---|---|---|
| `--config <directory>` | `/etc/primos-witness` | must exist |
| `--node-id <id>` | `1023` | 0–1023. **Fixed once deployed** — see below |
| `--bind <host:port>` | from configuration | overrides `bind.host`/`bind.port` |
| `-h`, `--help` | | |
| `-V`, `--version` | | which build this box is running |

Exit codes, for whatever supervises it: **0** normal, **2** for a mistyped option or a missing
configuration directory — reported as one sentence and the usage text — and **1** for a failure to
start, which is logged with its cause.

**The node id is durable.** It is recorded in every served application's `votedFor`, and every
cluster must configure the same value as its witness peer's id. Changing it on a running witness
makes its recorded votes unattributable. Pick it once.

Each cluster is configured with the matching peer:

```java
.witness(Peer.witness(1023, "witness.example.com", 7420))
```

### systemd

```ini
[Unit]
Description=Primos witness server
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=primos
Group=primos
ExecStart=/opt/primos-witness/bin/primos-witness --config /etc/primos-witness
Restart=on-failure
RestartSec=5s

# The state directory must survive. Everything else may be locked down.
ReadWritePaths=/var/lib/primos-witness
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

`SIGTERM` is a clean shutdown: the loop finishes its turn, tells every served cluster it is going
away so none of them waits out a probe timeout, and closes its state stores before exiting. Give it
a moment rather than following up with `SIGKILL`.

### Firewall

One inbound UDP port (7420 by default) from every node of every served cluster. Replies leave from
the same socket, so no separate outbound rule is needed, and that is also what keeps NAT pinholes
open in both directions.

---

## Configuring

A directory, not a single file:

```
/etc/primos-witness/
├── witness.properties
└── applications/
    ├── orders.properties
    └── billing.properties
```

The split earns its keep on shared infrastructure: adding a cluster is a **file drop** rather than
an edit to a file every tenant contends over, and each team's secret can carry its own permissions,
which one shared file makes impossible.

### `witness.properties`

| key | default | hot-reloadable |
|---|---|---|
| `bind.host` | `0.0.0.0` | **no** — read and ignored until restart |
| `bind.port` | `7420` | **no** |
| `state.directory` | `<config>/state` | **no** |
| `registration.mode` | `strict` | yes |
| `registration.limit` | `64` | yes |
| `secret` | none | yes |
| `secret.algorithm` | `HmacSHA256` | yes |
| `timings.profile` | `lan` | yes |
| `reload.debounce.millis` | `250` | yes |
| `reload.rescan.seconds` | `30` | yes |

`timings.profile` is `lan` or `wan` — a named profile rather than several separate intervals,
because it is the *relationships* between them that have to hold. A witness genuinely on a third
network wants `wan`: a 50ms WAN round trip against a 300ms election timeout is only 6×, where Raft
wants an order of magnitude.

**Set the same profile on the witness and on every node of the clusters it serves.** A node refuses
to grant a *pre-vote* while it has heard from a leader within `electionTimeoutMin`, so a `wan`
witness among `lan` nodes is five times more reluctant to agree to a failover than its peers — and
for a two-node pair, the witness's vote is the one everything is waiting on. Nothing is corrupted
by a mismatch; failover just gets slower in a way that is very hard to see from either side.

The full derivation, including the garbage-collection floor that matters more than the network on
a JVM, is in the [project README](../README.md#timing).

### `applications/<name>.properties`

**The application name is the filename.** `orders.properties` serves the cluster named `orders`.
That is deliberate — the name cannot disagree with the file's own contents. It is validated against
the wire's rules at load (printable ASCII, no spaces, at most 32 bytes) and rejected rather than
silently truncated.

| key | notes |
|---|---|
| `secret` | falls back to the witness-wide `secret` if absent |
| `secret.previous` | also accepted, for rotation |
| `secret.algorithm` | the JCE `Mac` name, e.g. `HmacSHA512`; falls back to the witness-wide one, which defaults to `HmacSHA256` |

An empty file is valid and means "serve this cluster with the witness-wide secret".

`secret.algorithm` is per application because a witness serves clusters it does not own: one of
them moving to another algorithm must not be a flag day for every other tenant. **It has to match
what that cluster's nodes were built with** — nothing on the wire says which algorithm produced a
tag, so a mismatch looks exactly like a wrong secret: every datagram fails verification and is
counted under `BAD_TAG`. Almost nobody should set this; it is here for a deployment whose policy
names something else, and for the day SHA-256 stops being above suspicion.

### Secrets

Four forms, so configuration can go into version control while the secret does not:

```properties
secret = inline-value
secret = env:ORDERS_WITNESS_SECRET
secret = file:/etc/primos-witness/secrets/orders
secret = base64:aGVsbG8gd29ybGQ=
```

The loader warns about world-readable files, the way `ssh` does. Keep them `0640` and owned by the
service user.

### Rotating a secret

Two edits with a window in between, no restart, no flag day:

1. On the witness, set `secret` to the new value and `secret.previous` to the old one. It now
   accepts either.
2. Roll the cluster's nodes onto the new secret at your own pace. **Each node needs both during
   the window** — the witness tags its replies with the *current* secret, so a node holding only
   the old one could send but never hear an answer.
3. Remove `secret.previous`. The old secret stops working.

Do not skip step 3. The window is a rotation window, not a permanent second key.

### Hot reload

The configuration directory is re-read while the server runs. **This is the point of the design**,
because restarting a witness is the one genuinely risky operation in its lifecycle: while it is
down, every two-node cluster it serves is back to tolerating zero failures.

- A whole-directory rescan on any file event, debounced ~250ms, so one save is one reload.
- A periodic rescan (~30s) as a genuine safety net — file watching is inotify on Linux but a slow
  polling fallback on macOS, and does not work at all on some network filesystems.
- **A malformed file never takes down the running configuration.** The candidate is parsed and
  validated whole, and published only if it is good; otherwise the last good configuration keeps
  serving and the failure is logged loudly.

Keys marked not hot-reloadable are read and ignored with a log line, rather than half-applied.

---

## Registration modes

### `strict` (default)

Serves exactly the applications configured in `applications/`, and drops everything else, counted
and logged once per unseen name.

### `permissive`

Serves any application name it hears, creating state for it. Less operational burden, a legitimate
choice — and the way a typo becomes a split brain.

**The walkthrough that is the reason `strict` is the default:**

> Two clusters are deployed. One is configured `orders`. The other, through a typo in a config
> file, is configured `ordres`.
>
> The two sets of nodes drop each other's datagrams, because the application name does not match —
> the cross-talk protection working exactly as designed. So each side sees the other as
> unreachable, and each asks the witness for a vote.
>
> A permissive witness auto-creates **two** state buckets, one per name, and grants a vote from
> each. Both sides now have a majority. **Two leaders, out of one typo** — and the very protection
> that should have caught the mismatch is what hides the cause, because neither cluster ever sees
> the other's traffic.

Permissive mode is hardened rather than merely documented:

- every auto-registration logged at WARN with the name and source address;
- **near-miss detection** on first sight of a name — case-insensitive, small edit distance against
  names already served — warning explicitly that it may be a typo. It also runs in strict mode,
  where a dropped `ordres` next to a served `orders` is the entire diagnosis in one line;
- `registration.limit`, so an open witness cannot be filled up by whoever can reach the port;
- state is **never** auto-expired, because deleting `votedFor` for a dormant cluster that later
  returns is precisely the double-vote bug.

---

## Operating

### What it logs

At startup:

```
serving 2 application(s): billing, orders
registration mode is STRICT
witness 1023 bound to /0.0.0.0:7420, STRICT mode, state in /var/lib/primos-witness
cluster 'orders': witness 1023 is serving, with the voter set learned from traffic
witness 1023 is serving 2 application(s)
```

Worth alerting on:

| level | message | means |
|---|---|---|
| WARN | `no applications configured` | strict mode with an empty `applications/` serves nobody |
| WARN | `no secret is configured` | see rule 3 |
| WARN | `... is world-readable and may contain a cluster secret` | fix the file mode |
| WARN | `dropping traffic for unserved application 'X'` | a cluster is asking and being refused |
| WARN | `'X' is one or two characters from 'Y'` | **near miss — check both configurations** |
| WARN | `auto-registering application 'X'` | permissive mode serving someone unconfigured |
| WARN | `refusing to auto-register` | `registration.limit` reached |
| ERROR | `cannot read the configuration in ...` | a bad edit; the last good config is still serving |
| ERROR | `application 'X' has failed` | that cluster is no longer served — see below |

### One application failing does not stop the server

If an application's state store fails — a full or broken disk — that application is **quarantined**:
it stops being served, and every other tenant keeps working. One team's storage problem must not be
every team's failover outage.

The clusters relying on that application's witness are back to the fault tolerance they have
without one. Fix the cause and restart the witness.

### State on disk

```
/var/lib/primos-witness/
├── orders/{state,state.lock}
└── billing/{state,state.lock}
```

A directory per application, so one cluster's state can be backed up, moved or removed on its own,
and two applications cannot share a file lock and thereby a failure. `state.lock` is an exclusive
lock — a second witness pointed at the same directory fails to start rather than quietly
double-voting.

Back it up if you like, but **never restore an old copy**: restoring a stale `votedFor` is
indistinguishable from forgetting a vote. If in doubt, an empty state directory for a *new* witness
identity is safer than a stale one for an existing identity.

### Removing an application

Delete `applications/<name>.properties`. It stops being served within one reload cycle. Its state
directory stays; remove it by hand only once you are certain the cluster is gone for good.

---

## Troubleshooting

**A cluster cannot reach the witness.** Its nodes log `the witness (N) is unreachable`. Check UDP
reachability on the port first, then that the witness logs the application as served, then that the
node ids match — the witness's `--node-id` must equal the id each cluster configures with
`Peer.witness(id, ...)`.

**The witness sees the traffic but never answers.** Almost always the secret. A datagram failing
authentication is dropped silently before anything is parsed, by design, so look at the drop
counters rather than for an error. Check for a mismatched per-application secret, or a rotation
where the node holds only the old one and the witness has already dropped `secret.previous`.

**`dropping traffic for unserved application 'X'`.** Strict mode, and no `applications/X.properties`
exists. If `X` looks like a near miss of something you do serve, treat it as a typo in that
cluster's configuration and not as something to fix by adding a file.

**Two leaders in one cluster.** Check, in order: two witness instances answering for the same
identity (rule 1); a wiped or restored state directory (rule 2); two clusters whose names differ by
a typo on a permissive witness (the walkthrough above).

**Failover is slower than expected.** The witness's `timings.profile` should match the deployment —
`wan` if it sits on another network. Note the witness does not campaign, so it never causes an
election; it only answers.

---

## What it does not do

No replication, no state machine, no snapshots, no runtime membership changes. The witness never
campaigns, never becomes leader, and never appears as one to any listener. It votes, and that is
all.

## License

Apache License 2.0 — see [LICENSE.txt](../LICENSE.txt).
