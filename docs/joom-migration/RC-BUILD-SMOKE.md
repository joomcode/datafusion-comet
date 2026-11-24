<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Building and qualifying release JARs

The manual **RC Build & Smoke** workflow is
`.github/workflows/rc_build_smoke.yml`. It builds entirely on native GitHub
runners, verifies the final dual-architecture JARs, and neither creates tags nor
publishes to JFrog/production. Current accepted source, runs and immutable JAR
checksums are in [QUALIFICATION.md](QUALIFICATION.md); deployment requirements are
in [RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md).

## Manual workflow contract

In GitHub Actions choose **RC Build & Smoke → Run workflow → branch/tag** and
supply `expected_commit` as the full, lowercase 40-character SHA of that exact
ref. The source gate rejects a different SHA before building. A moving branch
must still resolve to that requested SHA when dispatched. Reusing a successful
run for different source/JAR bytes is forbidden.

Seven jobs run in dependency order:

1. **Source:** exact ref/SHA, clean tracked/untracked source, Cargo lockfile hash,
   actual Temurin 25.0.5.1+2, actionlint, CI-config checks and release-tool unit tests.
2. **Native AMD64 + ARM64:** native Ubuntu 24.04 runners invoke the existing
   `dev/release/joom/Dockerfile` Ubuntu 20.04 builders. Rust 1.100.0,
   `cargo build --locked --release -p datafusion-comet`, GCC 10, Protobuf 30.2,
   Java 17 builder and Java 17 bytecode preserve the GLIBC <=2.31 contract.
   AMD64 uses `-Ctarget-cpu=x86-64-v3`; ARM64 has no extra CPU flags. Record source,
   lockfile, compiler, builder image, dependency graph, ELF imports and SHA256.
3. **Package once:** verify both native inputs before Maven, use the effective
   Spark4.2/Delta POM and the complete Maven reactor, then produce one core JAR
   containing both native libraries and a separate Delta adapter. Portable test
   support/dependency classpaths are resolved in that same reactor session.
4. **Packaged smoke AMD64 + ARM64:** download that unchanged candidate, install
   the exact Temurin 25 runtime on each native Ubuntu 24.04 runner, and execute
   the existing final-JAR tests. No Maven rebuild or repackaging is allowed.
5. **Qualified export:** validate both smoke receipts against the same commit,
   lockfile, JAR/native/package/support hashes and required session configuration;
   export only those exact bytes.

The current `CASES` in `dev/release/joom/verify-packaged-runtime.py` contain
**27 cases per architecture**: Delta4, DPP1, reload1, ANSI6, G08 UDF1,
G08 Delta serialization2, G09 cost4, wide-sort1, wide-shuffle1, range1,
G07 Kryo1, G10 UDF shuffle2 and G10 expression guards2. Zero failed, canceled or
ignored cases are accepted. The helper checks real vendor/build, required session
defaults, loaded core/Delta classes and the bundled native library's origin/hash.
It includes mixed-engine results and required errors; native wrapper names alone
are not evidence that the bundled native implementation executed.

The release builder's Java 17 provenance must not be relabeled Java 25. Ordinary
CI's Cargo `ci` library is different from the release-profile `.so`; full CI and
packaged smoke serve complementary purposes. See [VALIDATION.md](VALIDATION.md).

## Local stages and recovery

Run from a clean Comet checkout at the selected source commit. Choose a new output
directory, for example `target/joom-release/<candidate-name>`; do not overwrite a
prior qualified output. These are recipes, not an instruction to execute builds
or network operations without the applicable authorization:

```shell
# All native and packaging stages locally (Docker required).
dev/release/joom/build-release.sh target/joom-release/<candidate-name>

# Or separate stages on native runners using one shared assembled output.
dev/release/joom/build-release.sh --native amd64 target/joom-release/<candidate-name>
dev/release/joom/build-release.sh --native arm64 target/joom-release/<candidate-name>
PREPARE_SMOKE=1 dev/release/joom/build-release.sh --package amd64 \
  target/joom-release/<candidate-name>
```

`--package` selects only the Maven builder architecture; both libraries must already
exist and must match the current source/lockfile/provenance. It never rebuilds a
missing native stage. The all-in-one mode defaults to an ARM64 packaging builder.
On an ARM64 laptop AMD64 emulation may be slow or hang in child-JVM launch; use
native AMD64 GHA rather than treating a stalled emulated run as passing.

The script refuses dirty source, changed HEAD/lockfile and an existing final
`sha256.txt`; the native provenance validator rejects stale, cross-commit,
incomplete or damaged inputs. Packaging cleans the effective POM's module
`target` directories but preserves the root Spark fixture and caller outputs.
Focal Git 2.25.1 needs the staging `/etc/gitconfig:ro` safe-directory mount;
it does not change the host Git config.

Useful controls are `DOCKER_CONTEXT`, `M2_DIR` (default `${HOME}/.m2`),
`MVN_OFFLINE=1`, `CARGO_JOBS` (default4), `TARGET_DIR`, `WORK_DIR`,
`CARGO_REGISTRY` (per-architecture registry volume prefix) and `CARGO_GIT`
(shared Git-source cache). `PREPARE_SMOKE=1` creates matching test support.
Keep source/dependency caches separate from disposable outputs; see
[CLEANUP.md](CLEANUP.md). Do not broadly clear shared caches to resolve a failure.

`verify-release.py` checks assembled provenance/JARs; `verify-packaged-runtime.py`
implements `prepare` and `run`; `verify-rc-result.py` binds both receipts and
exports the qualified directory. Use each script's `--help` for its exact input
paths. Retrying an infrastructure-only failed job is legitimate only if the
actual source and all reused artifact producers remain correctly identified.
A changed candidate requires fresh matching evidence; an old smoke receipt
cannot qualify replacement bytes.

## Delivery and durable provenance

Runtime files are:

- `comet-spark-spark4.2_2.13-1.2.0.jar` — Comet plus bundled Linux AMD64/ARM64 `.so`.
- `comet-contrib-delta-spark4.2_2.13-1.2.0.jar` — Comet Delta adapter; official
  `io.delta:delta-spark_4.2_2.13:4.4.1` and its dependencies are required separately.

The native libraries embed DataFusion and native Arrow/Parquet 59.3.2. JVM
Arrow/Parquet versions remain 18.3.0 / 1.17.0. Deliver the final JARs together
with `commit.txt`, `sha256.txt`, `build-manifest.json`, effective POM,
architecture-specific Cargo metadata/toolchain/ELF/native provenance and
`release-qualification.json` plus both smoke receipts/logs. Preserve the artifact
ZIP's GitHub digest and validate it on download as well as the files inside it.

Workflow artifact names are `rc-native-<arch>-<SHA>` (3 days),
`rc-candidate-<SHA>` (7 days), `rc-smoke-<arch>-<SHA>` (14 days) and
`rc-build-smoke-<SHA>` (14 days). The last is the accepted qualified export, not
the intermediate candidate. Download durable evidence before GitHub retention
expires. Caches are restore-only in this workflow.

## Tags and optional JFrog publication

Tags denote immutable source commits. Existing `joom-1.2-1-rc1` is historical;
the stable `joom-1.2-1` has **not** been authorized or created by this maintenance
work. Stable tagging, JFrog publication and production deployment remain separate
explicit user decisions. Neither cleanup nor successful CI grants permission.
Do not move an existing tag or relabel a JAR built at another commit merely to
match the branch's newer documentation-only HEAD.

The existing publication interface is:

```shell
# Local validation only; requires the selected tag to already exist locally.
dev/release/joom/publish-jfrog.sh --validate-only <joom-version> <qualified-out-dir>
```

The script accepts only `joom-1.2-N` or `joom-1.2-N-rcN`, requires the tag's commit
to equal `commit.txt`, derives the lockfile from that tag and validates the release
before accessing the network. A real publication (without `--validate-only`)
requires `JFROG_AUTH`, a base64 `user:token` credential for `https://jfrog.joom.it`;
never commit or print it. It checks that neither artifact version exists, uploads
JARs and generated POMs to `joom-maven-local` with checksums, then downloads through
`joom-maven` and compares bytes. It renames the JAR for Maven publication without
rebuilding or changing its contents; its generated POM is minimal, so consumers
must supply Spark/Delta runtime dependencies separately. A partial failed upload
is not a successful release and the overwrite guard must not be bypassed.

Maven coordinates are
`org.apache.datafusion:comet-spark-spark4.2_2.13:<joom-version>` and
`org.apache.datafusion:comet-contrib-delta-spark4.2_2.13:<joom-version>`.
No tag creation or publication command is run as part of documentation maintenance.
