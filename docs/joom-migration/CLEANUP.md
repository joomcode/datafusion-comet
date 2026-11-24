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

# Build resources, evidence and optional cleanup

This is an inventory and manual guide. The documentation consolidation deletes
obsolete tracked migration documents only. It does not delete downloaded files,
build outputs, caches, images, volumes, evidence, checkouts or Git refs. A parent
directory named temp does not make its contents disposable.

## Retain before cleanup

- Current accepted Java 25 bundle: `target/joom-release/gha-45292086072`, all 97
  files, not just the two JARs. Keep native libraries, commit/lock/build manifests,
  SHA256 lists, effective POM, both architecture smoke receipts and qualification.
  Source and JAR hashes are in QUALIFICATION.md.
- Immutable Java 25 qualification evidence under
  `${MIGRATION_AUX_ROOT}/20261123_comet12-java25`, especially
  `20261124_final-closeout/completion.json`, actual source/execution manifests,
  original report/SQL oracle proofs, API inventories and real retry producer
  bindings. The downloaded ZIP/API-digest receipt verifies the extracted bundle.
- Java 17 G10 bundle `target/joom-release/gha-45084227984` and its source-bound
  evidence under `${MIGRATION_AUX_ROOT}/20261121_comet12-g10`, especially
  `20261123_final-closeout/completion.json`. It is historical evidence, not a
  substitute for the Java 25 qualification.
- Actual integration receipts under the dated g10/java25 integration directories,
  and any evidence needed to reproduce a production finding. GitHub workflow
  retention is finite; do not rely on hosted artifacts being available forever.
- User source checkouts, SDKMAN JDKs, installed tools and shared caches.

## Recorded disk snapshot

These are allocated-size snapshots, not a fresh measurement or attribution of
every byte to this task. Nested values are already included in their parent.

| Resource                             | Recorded size              | Time / meaning                                                 |
| ------------------------------------ | -------------------------- | -------------------------------------------------------------- |
| `native/target`                      | 78,381,560 KiB (74.75 GiB) | 2026-11-24; accumulated native compiler/test/benchmark output  |
| Root `target`                        | 12,870,472 KiB (12.27 GiB) | 2026-11-24; includes nested RC outputs and diagnostic worktree |
| Java 25 auxiliary directory          | 682,044 KiB                | 2026-11-24 11:11 UTC, before the final small report downloads  |
| G10 auxiliary directory              | 9,144,232 KiB (8.72 GiB)   | 2026-11-23; includes diagnostics and qualification evidence    |
| Current Java 25 RC directory         | 519,852 KiB                | Nested in root target at the 2026-11-24 snapshot               |
| Earlier Java 25 RC `gha-45281150607` | 519,880 KiB                | Historical source955 candidate; not current qualification      |

Exact historical resource inventories remain recoverable from Git baseline
`586bbbf614823d70535a9ec1a384ea63aff7f4e0:docs/joom-migration/cleanup-ledger.json`.
The Java 25 auxiliary `storage-inventory.json` records its exact snapshot.
Consolidation creates only small documentation/review files under
`${MIGRATION_AUX_ROOT}/20261124_comet12-docs-consolidation`; no runtime rebuild,
new package environment or image download is needed.

## Optional build-output cleanup

Review each exact target, preserve artifacts/evidence and stop related builds
first. Rebuilding later will cost time. Do not delete the entire root target:
it contains release evidence and a linked diagnostic Git worktree.

```bash
(
set -eu
cd -- "${COMET_ROOT:?Set the intended Comet checkout}" || exit 1
test -f pom.xml && test -f native/Cargo.toml && test -z "$(git rev-parse --show-prefix)" || exit 1
# Optional commands below, after the checks above succeed:
rm -rf -- native/target
rm -rf -- target/joom-spark-4.2.1
rm -rf -- target/joom-release-validator-cases
rm -rf -- target/joom-release/joom-1.2-1-rc1/cargo-target target/joom-release/joom-1.2-1-rc1/.work
)
```

`target/joom-spark-4.2.1` is the task-created Spark SQL test fixture; it is not the
user's original Spark checkout. Native sources/vendor and final JARs are separate
from `native/target`. Inspect standard `spark/target`, `common/target`,
`contrib/delta-spark/target`, `native/target/criterion` and `target/criterion`
individually if more space is required; retain reports needed for diagnosis.

The diagnostic worktree `target/joom-runtime-memory-diagnostic` on
`joom/1.2-runtime-memory-diagnostic` must be removed through Git after preserving
its diagnostic state, before considering any enclosing directory removal:

```bash
git -C "${COMET_ROOT:?Set the intended Comet checkout}" worktree list
git -C "${COMET_ROOT:?Set the intended Comet checkout}" worktree remove target/joom-runtime-memory-diagnostic
```

No force is used; resolve any local changes if Git refuses. Do not delete primary
branches, qualified refs or backup refs as a side effect of freeing build space.

## Docker resources created by the migration

After no related containers/builds remain, the task-specific resources below can
be inspected and individually removed. Record actual identities before removal;
tags may have moved since the snapshot.

| Image tag                            | Recorded image ID                                                         |
| ------------------------------------ | ------------------------------------------------------------------------- |
| `comet-joom-builder-focal:1.2-arm64` | `sha256:de016db98defac03c0715462aeddb86738cb89b44c34656d6be0ce511f082e7c` |
| `comet-joom-builder-focal:1.2-amd64` | `sha256:92564ee97ddb02f4b12cf71243936c313dd774878603221f59abaac281095a49` |

```bash
docker --context desktop-linux image rm comet-joom-builder-focal:1.2-arm64 comet-joom-builder-focal:1.2-amd64
docker --context desktop-linux volume rm comet-joom-1.2-cargo-arm64 comet-joom-1.2-cargo-amd64 comet-joom-1.2-cargo-git
```

These three volumes were task-created, unlike pre-existing Spark Docker volumes.
Shared Docker build-cache/Desktop disk usage cannot be attributed wholly to this
migration. Do not run system/builder/volume prune or forced cleanup.

## Task-created local nightly toolchain

G10 installed `nightly-2026-11-21-aarch64-apple-darwin` with `miri` and `rust-src`
under `${HOME}/.rustup/toolchains`, recorded at 532,552 KiB. The default stable
toolchain was unchanged. Local Miri sysroot preparation was blocked; this install
is not evidence that Miri tests ran locally. Accepted Miri execution is in GHA.
Unlike the pre-existing user toolchains, this specific task-created toolchain is
an optional manual cleanup candidate if it is no longer needed:

```bash
rustup toolchain list
rustup toolchain uninstall nightly-2026-11-21
```

Do not remove the default/user toolchains or the entire `.rustup` directory.

## Downloaded bundles and auxiliary material

The Java 25 auxiliary root contains `qualified-artifact-45292086072.zip`
(197,930,477 bytes) and historical `qualified-artifact-45281150607.zip`
(197,859,653 bytes). After separately preserving and verifying the extracted
current bundle and archive digest receipt, a duplicate ZIP may be unnecessary.
An older downloaded artifact or extracted RC never qualifies newer source.

The following dated top-level directories are referenced by the original
migration inventory, plus the subsequent qualification/integration/documentation
tasks. Resolve them relative to `${MIGRATION_AUX_ROOT}`. Review contents and
ownership before deciding what can be archived or deleted; the list is NOT a
deletion command. Some are research sources for decisions rather than build waste.

- `20261115_comet-upstream-audit`
- `20261115_comet-upstream-branches-pr-audit`
- `20261115_datasci-spark4-audit`
- `20261115_spark4-code-audit`
- `20261115_spark4-comet-migration`
- `20261116_comet-ansi-pr5889-followup`
- `20261116_comet-build-ancestry-support`
- `20261116_comet-fork-ancestry`
- `20261116_comet-release-build`
- `20261116_comet12-branch-release-plan`
- `20261116_comet12-consolidated-plan`
- `20261116_comet12-execution-setup`
- `20261116_comet12-migration-register`
- `20261116_datasci-comet-selection-audit`
- `20261117_comet12-g01`
- `20261117_comet12-g01-ci-fixes`
- `20261117_comet12-g01-final-qualification`
- `20261117_comet12-g01-pr-qualification`
- `20261117_comet12-g01-resumed`
- `20261117_comet12-g01-runner-fix`
- `20261117_comet12-network-approval`
- `20261117_comet12-session-setup`
- `20261118_comet12-g01-finalization`
- `20261118_comet12-rc-workflow`
- `20261119_comet12-g02`
- `20261119_comet12-g02-integration`
- `20261119_comet12-g02-performance`
- `20261119_comet12-g03`
- `20261119_comet12-g03-integration`
- `20261119_comet12-g04`
- `20261119_comet12-g04-clippy-exception`
- `20261119_comet12-g04-independent-semantics`
- `20261119_comet12-portable-paths`
- `20261120_comet12-g04-integration`
- `20261120_comet12-g05`
- `20261120_comet12-g05-integration`
- `20261120_comet12-g06`
- `20261120_comet12-g06-integration`
- `20261120_comet12-g07`
- `20261120_comet12-g07-integration`
- `20261120_comet12-g08`
- `20261121_comet12-g08-integration`
- `20261121_comet12-g09`
- `20261121_comet12-g09-integration`
- `20261121_comet12-g10`
- `20261123_comet12-g10-integration`
- `20261123_comet12-java25`
- `20261124_comet12-docs-consolidation`
- `20261124_comet12-java25-image-check`
- `20261124_comet12-java25-integration`

The user-provided Iceberg checkout is a separate source at
`${MIGRATION_AUX_ROOT}/20261122_iceberg-source`; preserve it like the upstream
Comet/DataFusion/Spark and old Joom Spark checkouts. Do not clean other task
directories merely because their names resemble these entries.

Preserve standard shared `${HOME}/.m2`, `.cargo`, `.rustup`, `.sbt`, `.ivy2`,
Gradle, Coursier and ProtocBridge caches, installed Rust/Prettier/Git tools and
SDKMAN JDKs. No local Python environment or JDK was installed for Java 25
qualification. Builds and Python environments on GitHub runners do not consume
laptop storage. Avoid broad Maven clean, `git clean -fdx`, global cache deletion
or automatic Git ref deletion.

The remaining standard cache/configuration locations include `${HOME}/.gradle`,
`${HOME}/Library/Caches/Coursier`,
`${HOME}/Library/Caches/com.thesamet.scalapb.protocbridge.protocbridge`,
`${HOME}/.docker/buildx` and `${HOME}/.sdkman/candidates/java`.
These are shared/user resources, not task-owned cleanup targets.
