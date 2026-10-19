#
# Copyright © 2026 Lolaf.org
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# The witness as a container image, built from the layered jar that `package` already leaves
# in target/. Nothing in the Maven build talks to a Docker daemon — `./mvnw verify` must stay green
# on a machine that has none — so this file is driven by hand or by CI:
#
#   ./mvnw -o -pl primos,primos-witness,primos-micrometer,primos-witness-boot package -DskipTests
#   docker build -f src/main/docker/Dockerfile -t primos-witness:dev primos-witness-boot
#
# Buildpacks (`spring-boot:build-image`) would do this in one command and were rejected: they fetch
# at build time and produce an image whose contents nobody chose, where the posture here is a base
# you can name and nothing pulled at run time.

ARG JRE_IMAGE=eclipse-temurin:17-jre-alpine

# --- extract ----------------------------------------------------------------------------------
#
# The JRE is the only tool needed: Boot's `tools` jarmode is in the jar itself.
FROM ${JRE_IMAGE} AS extract

WORKDIR /build
# One jar, because .dockerignore beside this file narrows the context to it — the sources and
# javadoc jars sit in the same target/ and would make this glob ambiguous.
COPY target/*.jar application.jar
# --layers splits the jar along its layers.idx, so the copies below land in cache order: the
# dependencies change rarely and this application changes every build. What comes out under
# application/ is application.jar, whose Class-Path is relative to itself, which is why the layers are
# copied back into one directory rather than kept apart.
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

# --- the image --------------------------------------------------------------------------------
FROM ${JRE_IMAGE}

LABEL org.opencontainers.image.title="Primos witness" \
      org.opencontainers.image.description="A leader-ineligible voter, so a two-node cluster can elect a leader." \
      org.opencontainers.image.url="https://github.com/lolaf-org/primos" \
      org.opencontainers.image.source="https://github.com/lolaf-org/primos" \
      org.opencontainers.image.licenses="Apache-2.0"

# A witness holds one identity's votes and needs no privileges to do it. Root is left behind before
# the first byte is served, and the root filesystem can be read-only: see docs/witness-spring-boot.md for the two
# writable paths (/tmp, and the state volume below).
RUN addgroup -S -g 1000 primos && adduser -S -u 1000 -G primos -H -D primos

WORKDIR /opt/primos-witness
# Three layers, not the four the idiom usually shows: Boot 4's extract leaves a plain executable jar
# whose Class-Path names lib/, so there is no loader to copy and spring-boot-loader/ comes out empty.
# The order is cheapest-changing first, which is the whole point of extracting at all.
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./

# The state directory must outlive the container, and this line is not enough on its own — an
# anonymous volume is only marginally better than the writable layer, since the next `docker run`
# gets a fresh one. It is here so that a run without `-v` still does not write votes into a layer
# that is discarded at `docker rm`; every documented invocation carries its own `-v` or PVC.
#
# A witness that forgets currentTerm/votedFor across a restart can grant a second vote in a term it
# has already voted in, which elects two leaders — the exact failure it was deployed to prevent.
RUN mkdir -p /var/lib/primos-witness && chown primos:primos /var/lib/primos-witness
VOLUME /var/lib/primos-witness

USER primos:primos

# The /udp is the whole of it. `-p 7420:7420` publishes TCP, and the witness is then simply never
# spoken to — total silence, which is indistinguishable from a partition, and a partition is what a
# witness exists to survive, so it will not look wrong.
EXPOSE 7420/udp
# The actuator, which a readiness check reads. Never a liveness probe — see below.
EXPOSE 8080/tcp

# JDK_JAVA_OPTIONS rather than flags on the ENTRYPOINT, so that `-e JDK_JAVA_OPTIONS=...` replaces
# these outright instead of being overridden by them. The java launcher prints a NOTE to stderr
# saying it picked them up; that line is expected.
#
# A percentage rather than -Xmx: the container limit is the one number an operator sets, and a fixed
# heap silently ignores it being raised. The tarball's launcher says -Xmx128m, which sizes a process
# with no framework in it — sizing this container to that number is an OOM kill that looks like a
# crash. The limit wants real headroom above the heap; docs/witness-spring-boot.md says 512Mi and where it comes from.
#
# ExitOnOutOfMemoryError stays. A witness that cannot answer must die and be seen to die, rather
# than hold its socket bound while it thrashes: a witness that is silently not answering looks
# exactly like a witness nobody is asking.
ENV JDK_JAVA_OPTIONS="-XX:MaxRAMPercentage=65.0 -XX:+ExitOnOutOfMemoryError"

# No HEALTHCHECK, deliberately, and phase 17 giving this packaging an endpoint to point one at does
# not change it. Docker restarts nothing on a failed healthcheck, but every orchestrator built on
# top reads it as a liveness signal, and **restarting a witness is the one genuinely risky operation
# in its lifecycle**: while it is down, every two-node cluster it serves tolerates zero failures. A
# probe that can be wrong — a slow disk, a paused JVM, a blip in the probe path — is then a
# mechanism for causing the outage it is watching for. Readiness is fine and useful; liveness is not.

# Exec form, so java is PID 1 and receives SIGTERM itself: no init shim, and no shell to swallow the
# signal. Boot's graceful shutdown then drives the server's stop(), which sends every served cluster
# its Leaving datagram — so none of them waits out a probe timeout — and closes the state stores.
# That takes longer than Docker's 10s default grace, which is why the documentation says --stop-timeout 30.
ENTRYPOINT ["java", "-jar", "application.jar"]
