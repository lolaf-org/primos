# primos-witness-boot

The witness as a Spring Boot application: actuator health, configuration from the environment, and
the packaging [the container image](#the-container-image) is built from. A front door onto
[`primos-witness`](../primos-witness), which is the witness itself; the other one is the tarball in
[`primos-witness-cli`](../primos-witness-cli).

**The operator manual is [`primos-witness/README.md`](../primos-witness/README.md).** Almost
everything worth knowing — the three safety rules, the keyring, `config.source`, hot reload, what to
alert on — describes server behaviour every packaging shares, so it lives with the server. This file
covers only what is different here.

## Which packaging

| | `primos-witness-cli` | this module |
|---|---|---|
| deployment | a tarball and a systemd unit | a container image, or the jar |
| configuration | `witness.properties` and `applications/` | the same, plus Spring's environment |
| health, metrics | the log | `/actuator/health`, and `/actuator/metrics` per served cluster |
| dependencies in the process | picocli and slf4j-simple | a Spring Boot application, with Tomcat |
| **startup, measured** | **~0.44s** | **~4.0s** |

The startup row is not a detail: **the window in which a witness is starting is the window in which
every two-node cluster it serves tolerates zero failures.** Both numbers are small and the ratio is
about nine to one; which matters depends on how often you restart a witness, and the answer should
be "almost never". Choose this packaging because you already run containers and want the health
endpoint and environment configuration; choose the tarball because you want a process with no
framework in it that can sit untouched for years.

Neither front door holds any logic of its own. Both build a `WitnessServer` and run it; if either
grows behaviour, or anything is copied between them, that is the thing to delete.

## Quick start

```bash
java -jar primos-witness-boot-<version>.jar \
  --primos.witness.config-directory=/etc/primos-witness
```

The directory is the one the manual describes: `witness.properties` beside an `applications/`
directory with a file per served cluster. Everything in it is re-read while the server runs, so
onboarding a cluster never needs a restart.

Spring's shutdown stops the server: each served cluster is sent its `Leaving` datagram and every
state store is closed before the JVM exits. Give the process time to do it —
`terminationGracePeriodSeconds` or `--stop-timeout` must exceed
`spring.lifecycle.timeout-per-shutdown-phase`, which defaults to 30s and bounds the wait.

## Configuration

**Spring binds three settings, and the rest stay in the file.** These three are the tarball's three
command-line options, under the names `witness.properties` already uses:

| property | environment variable | what it is |
|---|---|---|
| `primos.witness.config-directory` | `PRIMOS_WITNESS_CONFIGDIRECTORY` | `--config`; default `/etc/primos-witness` |
| `primos.witness.node-id` | `PRIMOS_WITNESS_NODEID` | `--node-id`; default `1023` |
| `primos.witness.bind.host` / `.port` | `PRIMOS_WITNESS_BIND_HOST` / `_PORT` | `--bind`; either half may be given alone, and the other comes from the file |

**Everything else — the state directory, `config.source`, the keyring, the timings, vote logging,
the rescan interval — comes from `witness.properties`, and that is a rule rather than an
omission.** Spring binds once, at boot. A key expressed in both places would freeze at whatever
Spring read while an operator edited the file and watched nothing happen — and the file is re-read
on purpose, because restarting a witness is the one genuinely risky operation in its lifecycle. The
keyring settles it on its own: a secret bound at boot cannot be rotated without a restart.

Setting one of those here anyway is refused at startup, with a message naming both places. It is not
ignored, because ignored is how the state directory ends up somewhere nobody expects and is
discovered when a restart cannot find a vote.

### The properties store, when there is no file at all

`primos.witness.config-store=properties` moves the whole configuration into Spring properties.
**It costs hot reload**, which is the reason the file store is the default: adding a cluster or
rotating a secret then means restarting the witness. It is a fair trade for a single-tenant
deployment whose served set genuinely never changes, and a bad one for anything else. Choosing it
logs a warning saying so.

```properties
primos.witness.config-store=properties
primos.witness.state-directory=/var/lib/primos-witness
primos.witness.bind.port=7420

primos.witness.applications[0].name=orders
primos.witness.applications[1].name=billing
# Per application, because a witness serves clusters it does not own and one of them on another
# network must not set the intervals for all of them.
primos.witness.applications[1].timings.profile=wan

# The default for any application that names none of its own, and what an application
# auto-registered under config.source=cluster starts on.
primos.witness.timings.profile=lan

primos.witness.keys.platform.value=${PLATFORM_WITNESS_SECRET}
primos.witness.keys.orders.value=${ORDERS_WITNESS_SECRET}
primos.witness.keys.orders.applications=orders-*,*-test
```

| property | what it is |
|---|---|
| `state-directory` | required; must be durable storage that survives a restart |
| `config-source` | `file` (serve only what is listed) or `cluster` (a cluster may create itself) |
| `cluster-application-limit` | how many `cluster` may create; default 64 |
| `applications[n].name` | a served cluster |
| `applications[n].timings.*` | its intervals, or the witness default below |
| `timings.profile` | `lan` or `wan` |
| `timings.heartbeat-interval`, `.election-timeout-min`, `.election-timeout-max`, `.probe-interval`, `.advertisement-interval`, `.failure-threshold` | the intervals spelled out — a profile **or** these, never both |
| `vote-logging` | who voted for whom, at INFO; default true |
| `keys.<name>.value` | the shared secret |
| `keys.<name>.algorithm` | the JCE `Mac`; default HMAC-SHA256 |
| `keys.<name>.applications` | the name patterns this key may speak for; `*` matches any run of characters |

The keyring rule is the manual's and is not relaxed here: **a name that any key claims is verified
only by the keys claiming it**, and a name nothing claims is verified by the keys that claim
nothing. Never run without a key.

Two spellings worth knowing, because nobody guesses them: a list index becomes
`PRIMOS_WITNESS_APPLICATIONS_0_NAME`, and durations are Spring's, so `250ms` and `2s` rather than
the file's `.millis` integers.

## Health

`/actuator/health` is `UP` exactly while this witness is serving, and its detail names what it
serves and what it has stopped serving:

```json
{"status":"UP","components":{"witness":{"status":"UP","details":{
  "boundAddress":"/0.0.0.0:7420","served":["orders","billing"],"quarantined":[]}}}}
```

**One quarantined tenant does not make the witness `DOWN`.** A witness serving fifty clusters with
one quarantined is doing its job for forty-nine, and a verdict that says otherwise takes all fifty
out of rotation over one of them. A quarantined application — most often one whose state can no
longer be made durable — belongs in the detail, where somebody can act on it. `DOWN` means one
thing: the loop is not running, so this process is answering HTTP and serving nobody.

**Do not point a liveness probe at it.** Restarting a witness is the one genuinely risky operation
in its lifecycle, so a probe that can be wrong is a mechanism for causing the outage it is watching
for. Spring Boot ships `/actuator/health/liveness` by default and **this module switches the probe
groups off** for exactly that reason; a readiness check needs no group and can read
`/actuator/health` directly. Boot's disk-space contributor is off too, and for the same reason: it
watches a directory this process does not care about, and nothing but the witness may answer whether
the witness is serving.

## Metrics

`primos-micrometer` is on the classpath, so `/actuator/metrics` carries the witness's own routing
counters and a full set of node meters **per served cluster**, tagged with that cluster's name and
this witness's node id. Nothing to configure; it binds when the application has a `MeterRegistry`.

The two worth alerting on:

- `primos.witness.routing{reason="undeliverable_replies"}` should stay at zero. A witness that
  silently fails to answer looks exactly like a witness nobody is asking, while the cluster on the
  other end waits out an election timeout for a vote that was never coming.
- `primos.witness.applications{state="quarantined"}` above zero is the health detail as a number.

The catalogue, and the tag rules, are in [`primos-micrometer`](../primos-micrometer/README.md).

## Startup

Measured on the packaged jar, from exec to serving, five runs: **~0.44s** for the tarball and
**~4.0s** here. `spring.main.lazy-initialization=true` brings this to ~3.6s and is **not** enabled:
0.4s does not buy moving a misconfiguration from startup to first use, in a server whose whole
posture is to fail loudly before it is trusted with a vote.

Adding the metrics was measured rather than assumed, since this is the number that matters most
here: binding them costs **~0.2s**, which is inside the run-to-run spread of the measurement.

## Build

```bash
./mvnw -o -pl primos,primos-witness,primos-micrometer,primos-witness-boot package -DskipTests
```

leaves a layered, runnable `target/primos-witness-boot-<version>.jar`, which is both the jar the
quick start runs and what the image below is built from.

## The container image

Two rules decide whether a containerised witness is safe, and **a container platform makes both
mistakes the default**. They are the manual's first two rules, restated for the platform that
breaks them:

- **One container per identity. Never two.** A `replicas: 1` StatefulSet, never a Deployment, never
  an HPA, never a Service load-balancing across more than one pod. Two witnesses answering for one
  cluster is a double vote by construction — each grants its own vote in the same term, and two
  nodes each believe they have a majority. Nothing outside the cluster can see it happen.
- **The state directory is a volume that outlives the container.** Never the writable layer, never
  an `emptyDir`, never a `tmpfs`. The witness fsyncs `currentTerm`/`votedFor` before answering any
  vote; one that forgets a vote across a restart or a reschedule can grant a second in a term it has
  already voted in, which elects two leaders — the exact failure it was deployed to prevent.

Everything else in this section is a consequence of one of those two, or of the fact that
**restarting a witness is the one genuinely risky operation in its lifecycle**: while it is down,
every two-node cluster it serves tolerates zero failures.

### Building it

```bash
./mvnw -o -pl primos,primos-witness,primos-micrometer,primos-witness-boot package -DskipTests
docker build -f primos-witness-boot/src/main/docker/Dockerfile -t primos-witness:dev primos-witness-boot
```

The `Dockerfile` is a plain multi-stage build over the layered jar — a named JRE 17 base, a
non-root user, and nothing fetched at run time. Not `spring-boot:build-image`: buildpacks fetch
at build time and produce an image whose contents nobody chose. **Nothing in the Maven build talks
to a Docker daemon**, so `./mvnw verify` stays green on a machine that has none; CI builds the
image in a separate job.

### Running it

```bash
docker run -d --name primos-witness \
  -p 7420:7420/udp \
  -p 127.0.0.1:8080:8080 \
  -v primos-witness-state:/var/lib/primos-witness \
  -v /etc/primos-witness:/etc/primos-witness:ro \
  -e PRIMOS_WITNESS_SECRET=... \
  --read-only --tmpfs /tmp \
  --memory 512m \
  --stop-timeout 30 \
  primos-witness:dev
```

Four of those lines fail silently if you drop them:

| | why |
|---|---|
| `7420:7420/udp` | without `/udp` the published port is TCP and the witness is **never spoken to**. Total silence, which is indistinguishable from a partition — and a partition is what a witness exists to survive, so nothing will look wrong |
| `-v ...:/var/lib/primos-witness` | rule 2. The image declares `VOLUME`, which stops votes landing in a layer `docker rm` discards, but an anonymous volume is fresh on the next `docker run`. Name it |
| `--stop-timeout 30` | Docker's default grace is 10s and the shutdown timeout is 30s, so a clean stop — a `Leaving` datagram to every served cluster, then the state stores closed — can be `SIGKILL`ed at the instant it finishes, and every one of those clusters then waits out a probe timeout for a peer that had already gone |
| `--memory 512m` | the heap is 65% of the limit (`JDK_JAVA_OPTIONS` in the Dockerfile). Sizing the container to the tarball launcher's `-Xmx128m` — which sizes a process with no framework in it — is an OOM kill that reads as a crash |

`--read-only` is safe and worth keeping: the only paths written are `/tmp`, which Tomcat wants, and
the state volume. The container runs as uid 1000 and needs no capabilities.

Worked examples, both carrying all of the above and commented line by line:
[`docker-compose.yml`](src/main/docker/docker-compose.yml) and
[`kubernetes.yaml`](src/main/docker/kubernetes.yaml) in `src/main/docker`.

### No `HEALTHCHECK`, and no liveness probe

The image ships neither, deliberately, and phase 17 having given this packaging an endpoint to point
one at does not change it — if anything it makes the rule easier to break.

Restarting a witness is the one genuinely risky operation in its lifecycle. A liveness probe that
can be wrong — a slow disk, a paused JVM, a blip in the probe path — is therefore a mechanism
for causing the outage it is watching for, and it fires at the moment its clusters are least able
to afford it. Docker restarts nothing on a failed `HEALTHCHECK`, but everything built on top of
Docker reads one as liveness, which is why there is not one to read.

**Readiness is fine**: point it at `/actuator/health`, which is `UP` exactly while this witness is
serving. Alert a human on it; do not let a machine act on it.

### Testing the image

```bash
docker build -f primos-witness-boot/src/main/docker/Dockerfile -t primos-witness:it primos-witness-boot
./mvnw -pl primos-witness-boot -Pdocker verify -Ddocker.image=primos-witness:it
```

`WitnessImageIT` drives a real cluster node at the published UDP port: it asks for a vote, checks
the vote reached the host volume rather than a layer, stops the container and asserts the departure
datagram went out and that the process exited on `SIGTERM` rather than being killed. It is bound
only in the `docker` profile and disables itself unless the image is named, so `./mvnw verify`
needs no daemon.
