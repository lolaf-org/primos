# primos-witness-cli

The witness server as a process you install: a picocli command line over
[`primos-witness`](../primos-witness), and a `tar.gz` an operator can unpack beside a systemd unit.

**The operator manual is [`primos-witness/README.md`](../primos-witness/README.md)**, and it ships
in the distribution as `MANUAL.md` so it is to hand on a box with no browser. Almost everything
worth knowing — the three safety rules, the keyring, `config.source`, hot reload, what to alert
on — describes server behaviour every packaging shares, so it lives with the server rather than
with a front door onto it. **This file covers what is particular to this one**: unpacking it, the
command line, and the unit that supervises it.

## Which packaging

| | this module | [`primos-witness-boot`](../primos-witness-boot) |
|---|---|---|
| deployment | a tarball and a systemd unit | a container image, or the jar |
| configuration | `witness.properties` and `applications/` | the same, plus Spring's environment |
| health, metrics | the log, and a `PrimosStatsListener` if you embed it | `/actuator/health`, and `/actuator/metrics` per served cluster |
| dependencies in the process | picocli and slf4j-simple | a Spring Boot application, with Tomcat |
| **startup, measured** | **~0.44s** | **~4.0s** |

That last row is not a detail: **the window in which a witness is starting is the window in which
every two-node cluster it serves tolerates zero failures.** Both numbers are small and the ratio is
about nine to one; it is the strongest argument for keeping this packaging alongside the
containerised one, together with "no framework anywhere near a process that holds every served
cluster's secret and is meant not to move for years".

Neither front door holds any logic of its own. Both build a `WitnessServer` and run it; if either
grows behaviour, or anything is copied between them, that is the thing to delete.

## Installing

Unpack the distribution wherever you keep such things:

```
primos-witness-cli-<version>/
├── bin/primos-witness
├── lib/*.jar
├── conf-sample/                 # heavily commented; copy it, do not point at it
│   ├── witness.properties
│   └── applications/example.properties
├── MANUAL.md                    # the operator manual, which is primos-witness/README.md
├── README.md                    # this file
└── LICENSE.txt
```

```sh
install -d -m 0750 /etc/primos-witness/applications
install -d -m 0750 /var/lib/primos-witness
cp -r primos-witness-cli-*/conf-sample/* /etc/primos-witness/
```

`conf-sample/` is deliberately a *sample* rather than the live path, so that upgrading never
overwrites `witness.properties` — an upgrade that replaced your secrets with placeholders would
take out every cluster the witness serves.

Requires a JRE 11 or newer on `PATH`, or `JAVA_HOME` set.

## Running

```sh
bin/primos-witness --config /etc/primos-witness
```

| option | default | notes |
|---|---|---|
| `--config <directory>` | `/etc/primos-witness` | must exist |
| `--node-id <id>` | `1023` | 0–1023. **Fixed once deployed** — the manual says why |
| `--bind <host:port>` | from configuration | overrides `bind.host`/`bind.port` |
| `-h`, `--help` | | |
| `-V`, `--version` | | which build this box is running |

Three options and no more, deliberately: everything else — the state directory, `config.source`,
the keyring, the timings — comes from `witness.properties`, re-read while the server runs.
A setting that could also be given here would be frozen at the value this process started with, and
the file is re-read on purpose, because restarting a witness is the one genuinely risky operation
in its lifecycle.

Exit codes, for whatever supervises it: **0** normal, **2** for a mistyped option or a missing
configuration directory — reported as one sentence and the usage text — and **1** for a failure
to start, which is logged with its cause.

## systemd

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
away so none of them waits out a probe timeout, and closes its state stores before exiting. Give
it a moment rather than following up with `SIGKILL` — systemd's `TimeoutStopSec` defaults to 90s,
which is ample.

## Build

```bash
./mvnw -o -pl primos,primos-witness,primos-witness-cli package -DskipTests
```

leaves `target/primos-witness-cli-<version>-dist.tar.gz`, laid out as *Installing* above shows.
