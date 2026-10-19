# Witness from the tarball

`primos-witness-cli` packages the witness as a process you install: a small command line and a `tar.gz` an operator
unpacks beside a systemd unit. Everything about the witness itself (the safety rules, the keyring, `config.source`, hot
reload, what to alert on) is in the [witness manual](witness.md), shipped in the tarball as `MANUAL.md`. This page
covers what is particular to the tarball: unpacking it, the command line, and the unit that supervises it.

## Which packaging

| | the tarball | [Spring Boot and containers](witness-spring-boot.md) |
|---|---|---|
| deployment | a tarball and a systemd unit | a container image, or the jar |
| configuration | `witness.properties` and `applications/` | the same, plus Spring's environment |
| health, metrics | the log | `/actuator/health`, and `/actuator/metrics` per served cluster |
| dependencies in the process | picocli and slf4j-simple | a Spring Boot application, with Tomcat |
| **startup, measured** | **~0.44s** | **~4.0s** |

The startup row is not a detail: **the window in which a witness is starting is the window in which every two-node
cluster it serves tolerates zero failures.** Both numbers are small and the ratio is about nine to one. Choose the
tarball for a process with no framework in it that holds every served cluster's secret and can sit untouched for years;
choose the image if you already run containers and want the health endpoint and environment configuration.

Both build the same witness from the same configuration directory, and neither holds any logic of its own.

## Installing

Unpack the distribution wherever you keep such things:

```
primos-witness-cli-<version>/
├── bin/primos-witness
├── lib/*.jar
├── conf-sample/                 # heavily commented; copy it, do not point at it
│   ├── witness.properties
│   └── applications/example.properties
├── MANUAL.md                    # the witness manual
├── README.md                    # this page
└── LICENSE.txt
```

```sh
install -d -m 0750 /etc/primos-witness/applications
install -d -m 0750 /var/lib/primos-witness
cp -r primos-witness-cli-*/conf-sample/* /etc/primos-witness/
```

`conf-sample/` is deliberately a *sample* rather than the live path, so that upgrading never overwrites
`witness.properties`: an upgrade that replaced your secrets with placeholders would take out every cluster the witness
serves.

Requires a JRE 11 or newer on `PATH`, or `JAVA_HOME` set.

## Running

```sh
bin/primos-witness --config /etc/primos-witness
```

| option | default | notes |
|---|---|---|
| `--config <directory>` | `/etc/primos-witness` | must exist |
| `--node-id <id>` | `1023` | 0 to 1023. **Fixed once deployed**; the [manual](witness.md#the-node-id-is-durable) says why |
| `--bind <host:port>` | from configuration | overrides `bind.host`/`bind.port` |
| `-h`, `--help` | | |
| `-V`, `--version` | | which build this box is running |

Three options and no more, deliberately. Everything else (the state directory, `config.source`, the keyring, the
timings) comes from `witness.properties`, which is re-read while the server runs. A setting that could also be given
here would be frozen at the value this process started with, and restarting a witness is the one genuinely risky
operation in its lifecycle.

Exit codes, for whatever supervises it: **0** normal, **2** for a mistyped option or a missing configuration directory
(reported as one sentence and the usage text), and **1** for a failure to start, which is logged with its cause.

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

`SIGTERM` is a clean shutdown: the loop finishes its turn, tells every served cluster it is going away, and closes its
state stores before exiting. systemd's `TimeoutStopSec` defaults to 90s, which is ample.

## Building it

```bash
./mvnw -o -pl primos,primos-witness,primos-witness-cli package -DskipTests
```

leaves `primos-witness-cli/target/primos-witness-cli-<version>-dist.tar.gz`, laid out as *Installing* above shows.
