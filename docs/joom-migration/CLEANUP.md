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

> **G01 finalization authorized:** documentation and history cleanup, followed by exact-SHA RC qualification, are in progress. Integration into `branch-1.2` and `joom-1.2-1-rc1` are conditional on successful checks. G02 and Java 21 work remain on hold.

# Cleanup after the migration

All commands below are for manual execution AFTER the entire migration and preservation of the required artifacts. Caches are still needed by subsequent groups and RCs. Nothing has been deleted automatically.

Keep the final JARs, both `.so` files, sha256.txt, commit.txt, build-manifest.json, effective-pom.xml, linux/* metadata/toolchain/ELF imports, smoke evidence and docs/joom-migration. Do not remove the entire root target directory: it contains release artifacts and the Spark fixture.

All Comet paths below are relative to the selected checkout root; external
paths use the explicit bindings in [PATHS.md](PATHS.md). Verify COMET_ROOT and
MIGRATION_AUX_ROOT before running any optional cleanup. The guarded shell
examples intentionally fail if the required root is unset or the selected
checkout is not a Comet repository root.

## Sizes

Measurements use du (allocated KiB); see cleanup-ledger.json for the detailed snapshot and timestamp. Nested sizes are included in their parent and must not be counted twice.

- `native/target`: 26.01 GiB.
- `target/joom-spark-4.2.1`: 0.80 GiB.
- `target/joom-release/joom-1.2-1-rc1`: 5.49 GiB.
- `target/joom-release/joom-1.2-1-rc1/cargo-target`: 4.94 GiB.
- `target/joom-release/joom-1.2-1-rc1/.work`: 0.00 GiB.
- `target/joom-release-validator-cases`: 0.00 GiB.
- `common/target`: 0.00 GiB.
- `contrib/delta-spark/target`: 0.00 GiB.
- `spark/target`: 0.39 GiB.
- `spark-integration/target`: 0.00 GiB.

## Targeted cleanup after the last RC

Native host build outputs, not source/vendor

```bash
(cd -- "${COMET_ROOT:?Set COMET_ROOT to the intended Comet checkout}" && test -f pom.xml && test -f native/Cargo.toml && comet_prefix=$(git rev-parse --show-prefix) && test -z "$comet_prefix" && rm -rf -- native/target)
```

Generated Spark qualification checkout; original user Spark checkout remains untouched

```bash
(cd -- "${COMET_ROOT:?Set COMET_ROOT to the intended Comet checkout}" && test -f pom.xml && test -f native/Cargo.toml && comet_prefix=$(git rev-parse --show-prefix) && test -z "$comet_prefix" && rm -rf -- target/joom-spark-4.2.1)
```

Release compiler intermediates only; keeps final JARs/manifests/native files/smoke

```bash
(cd -- "${COMET_ROOT:?Set COMET_ROOT to the intended Comet checkout}" && test -f pom.xml && test -f native/Cargo.toml && comet_prefix=$(git rev-parse --show-prefix) && test -z "$comet_prefix" && rm -rf -- target/joom-release/joom-1.2-1-rc1/cargo-target target/joom-release/joom-1.2-1-rc1/.work)
```

Synthetic negative-validator fixtures

```bash
(cd -- "${COMET_ROOT:?Set COMET_ROOT to the intended Comet checkout}" && test -f pom.xml && test -f native/Cargo.toml && comet_prefix=$(git rev-parse --show-prefix) && test -z "$comet_prefix" && rm -rf -- target/joom-release-validator-cases)
```

Only these task-exclusive builder tags, after no task containers remain; no force and no prune

```bash
docker --context desktop-linux image rm comet-joom-builder-focal:1.2-arm64 comet-joom-builder-focal:1.2-amd64
```

Only these task-created dependency volumes, after no task containers remain; no force and no prune

```bash
docker --context desktop-linux volume rm comet-joom-1.2-cargo-arm64 comet-joom-1.2-cargo-amd64 comet-joom-1.2-cargo-git
```

## Shared caches and sources

`~/.m2`, `~/.cargo`, `~/.rustup`, `~/.sbt`, `~/.ivy2`, Coursier and ProtocBridge were measured only as shared usage. Their entire size cannot be attributed to this migration; do not clear them wholesale. Preserve the user-installed JDK/Rust/Homebrew and source checkouts, including `${MIGRATION_AUX_ROOT}/src`.

Docker builder images: ARM de016db98def..., AMD92564ee97ddb...; exact IDs/sizes are in the ledger. The three named volumes did not exist in the baseline and were created by this task. Docker build cache/shared Desktop disk usage cannot be precisely attributed by owner. Do not use system/builder/volume prune, forced removal, root Maven clean or git clean -fdx.

## Auxiliary materials

Current logs/scripts/receipts: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-final-qualification`; previous: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-resumed`, `${MIGRATION_AUX_ROOT}/20261117_comet12-g01`. Historical audit directories and their purposes are listed in cleanup-ledger.json. After preserving results, a selected exact task directory may be removed manually; the name temp does not itself authorize deletion.

## G01 corrections after the first full CI

Auxiliary patches, local test logs, checkpoint and new GHA snapshots: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-ci-fixes`. This directory was created for the current task; preserve it until acceptance. The temporary pristine copy of 121 Spark files was already removed after equality was confirmed. Rebuilds reused existing `native/target`, `spark/target`, `contrib/delta-spark/target`, `target/joom-spark-4.2.1` and standard caches; do not remove them automatically. No final JARs were released by this iteration.

Snapshot after targeted checks: auxiliary directory ~6.3MiB; existing `native/target` ~39.7GiB total; Spark fixture with outputs ~831MiB. These are entire directory sizes, not the increase caused by the iteration.

The next follow-up GHA is stored separately in `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-ci-fixes/20261117_followup-gha`; the first run is frozen in the parent directory. Standard Maven caches and existing targets were also used for Scala2.12/Spark3.5 and Spark4.1 compatibility controls. No new Rust/Docker/release builds occurred in the follow-up. Full large SQL console logs were not retained: only selected lines, SHA receipts and bounded tails.

Final snapshot on 2026-11-18: G01 CI-fix auxiliary data ~21.4MiB, existing native/target ~39.7GiB, Spark fixture ~829MiB (entire sizes, not increases). Run44205897075 and all three attempts are preserved in `20261117_followup-gha`; only small API/log/report receipts were saved, without extracting large archives. Continued use may slightly increase the directory size. No automatic cleanup occurred; old release JARs are not the current qualified RC.

## Manual RC Build & Smoke: 2026-11-18

Currently checked runtime JARs/checksums/provenance/smoke: `target/joom-release/gha-44388252264` (~410MiB for the entire directory). Keep these until the migration ends; two JARs are used by the image. The older `target/joom-release/joom-1.2-1-rc1` belongs to source2b8f80b3f and is historical, not this checked artifact set.

Raw API/logs/control scripts/fault tests/actionlint: `${MIGRATION_AUX_ROOT}/20261118_comet12-rc-workflow`. It also contains the GitHub ZIP `qualified-artifact-44388252264.zip` (~181MiB); after archiving evidence and acceptance, that specifically selected task directory may be removed manually. No new local release/native builds occurred; Docker probes used existing images and --rm containers. Native builds and multi-gigabyte test dependencies ran on GHA; their caches were not downloaded to the laptop. Local RAT checks may have updated the ordinary M2 cache and `target/rat.txt`; preserve the shared cache.

GitHub artifact retention: native3 days, candidate/test support7 days, smoke/qualified14 days. Current qualified artifact ID14469302529; expiry is configured by the workflow, while the local copy does not expire. No JFrog/Docker-registry publication occurred.

## Final G01 translation, history and RC1 acceptance

- Auxiliary archive, mappings, checks, API snapshots, final ZIP and completion receipt: `${MIGRATION_AUX_ROOT}/20261118_comet12-g01-finalization`. Retain `documentation-before.zip` and `completion.json` until the handoff is verified.
- The final qualified output is `target/joom-release/gha-<run-id>`, where the final run ID is recorded in the annotated `joom-1.2-1-rc1` tag and `completion.json`. Expect approximately 410 MiB unpacked plus a 190 MiB downloaded ZIP. No local native rebuild is needed for this finalization.
- Preserve the final JARs, checksum/build/provenance and smoke receipts before any optional cleanup. Old `gha-44388252264` artifacts remain historical and are not the final rewritten RC1.
- Recovery refs `refs/joom-migration/checkpoints/20261118-before-finalization`, `refs/joom-migration/checkpoints/20261118-rebuilt-code` and the existing earlier checkpoint refs are local recovery evidence. Do not delete them automatically. No shared cache or user checkout is made disposable by this step.

After retaining the final handoff, optional deletion of only the final downloaded ZIP is:

```bash
(
  : "${MIGRATION_AUX_ROOT:?Set the approved auxiliary root}"
  : "${RC_RUN_ID:?Set the verified final RC run ID}"
  case "$RC_RUN_ID" in *[!0-9]*|'') exit 1 ;; esac
  test -f "${MIGRATION_AUX_ROOT}/20261118_comet12-g01-finalization/completion.json" || exit 1
  rm -- "${MIGRATION_AUX_ROOT}/20261118_comet12-g01-finalization/qualified-artifact-${RC_RUN_ID}.zip"
)
```

Set `RC_RUN_ID` to the verified final run ID. Consult `completion.json` for exact paths, archive/JAR sizes and hashes. Nothing is automatically removed.

## G02 local implementation and validation

Auxiliary evidence and snapshots: `${MIGRATION_AUX_ROOT}/20261119_comet12-g02`.
It contains build/test logs, earlier and final JVM reports, source hashes, reviews
and the final local completion receipt. Preserve it through final qualification.

The normal build outputs under `native/target` and `spark/target` were reused and
updated (approximately 47 GiB and 1 GiB respectively at the local checkpoint).
Those sizes include earlier project builds and are not all new G02 data. Shared
Maven/Cargo/JDK caches, the user's checkouts and G01 qualified release artifacts
were preserved. No new Docker image or release artifact was produced for G02.

After the migration and after preserving final release JARs/provenance, the
existing optional native/spark target cleanup described above also covers G02
outputs. Do not clean those caches during remaining feature implementation
unless the user asks; it would force substantial rebuilds. Nothing is deleted
automatically. Exact final local sizes are recorded in the G02 completion receipt.

G02 memory-wait follow-up evidence is under `${MIGRATION_AUX_ROOT}/20261119_comet12-g02/20261119_memory-wait`: positive and intentionally failing negative-control logs, JVM reports, source hashes, history-rewrite proof, final diff and push receipt. The negative-control source change is restored before the final passing build and is never committed. Reused native/spark targets and shared caches remain subject to the same preservation rules. No new Docker/release artifacts were created. Preserve this directory through final acceptance; nothing is automatically deleted.

## G02 bookkeeping performance comparison

Evidence/source copies/compiled classes/commands/raw logs/GC logs/results are under `${MIGRATION_AUX_ROOT}/20261119_comet12-g02-performance`. Dated run directories under its `20261119_results` contain Spark-managed temporary files. The benchmark uses the existing native dylib and cached dependencies; it created no release JAR, Docker image, production data or extra package cache. Preserve the report and raw evidence through migration acceptance. After preserving evidence, only this specifically selected task directory may be removed manually. Do not remove shared caches, source checkouts or native/target as part of this performance cleanup. Exact whole-directory sizes are in its completion.json.

## G02 accepted integration

Archive, service-change diff, ref snapshots, final RAT/audit and push/completion receipts: `${MIGRATION_AUX_ROOT}/20261119_comet12-g02-integration`. Recovery ref: `refs/joom-migration/checkpoints/20261119-g02-before-integration`. Preserve these together with the existing implementation/performance evidence until final migration acceptance; do not automatically delete recovery refs or shared caches. Integration creates no native build, Docker image or release JAR. Exact final auxiliary size and source identity are in completion.json.

## Portable documentation conversion

`${MIGRATION_AUX_ROOT}/20261119_comet12-portable-paths` contains the exact original
documentation archive, local-only root bindings, replacement/reference audits
and completion receipt. Only documentation and local audit files were created;
no builds, dependency downloads, artifact moves or automatic cleanup occurred.
Preserve the archive and bindings through acceptance so normalized paths remain
resolvable. Consult completion.json for the final identity and directory size.

## G03 sort/workspace implementation

`${MIGRATION_AUX_ROOT}/20261119_comet12-g03` contains source/port/review manifests,
local validation logs, the positive/negative ownership proof, benchmark results
and the final receipt. It includes a 437362352-byte copy of the G02 native debug
library for a possible existing SQL benchmark comparison; it is not a release JAR.
At the measured checkpoint the whole auxiliary directory used 427884 KiB.

Existing native/target used53881884KiB and spark/target
used1124536KiB in total; these include earlier builds and
are not G03 growth. Criterion manages outputs under the target/criterion paths
listed in cleanup-ledger.json. No new Docker image, toolchain installation or
release artifact was created. Keep this evidence, original shared caches, user
checkouts and existing qualified G01 JARs through final migration acceptance.
Nothing is automatically deleted; optional targeted cleanup remains manual.

## G03 accepted integration

`${MIGRATION_AUX_ROOT}/20261119_comet12-g03-integration` contains the original
documentation ZIP, source/ref manifests, acceptance audit, RAT/static-check logs
and push/completion receipts. Recovery ref:
`refs/joom-migration/checkpoints/20261119-g03-before-integration`. The earlier
G03 implementation receipt and raw test/benchmark evidence remain unchanged.
Preserve both directories and the recovery ref through final migration acceptance;
no automatic deletion is authorized. No runtime build, new release artifact or
Docker image is created by integration. Exact source/ref identities and auxiliary
size are recorded in the integration completion receipt.

## G04 local qualification

`${MIGRATION_AUX_ROOT}/20261119_comet12-g04` holds source/command manifests,
reviews, benchmarks, failed receipts and intentional ordering negative controls.
Preserve it through review/final acceptance; exact current auxiliary/shared-target
sizes are in `completion.json`. G04 reused `native/target`, `spark/target` and
existing caches; their full size is not G04 growth. Maven copied the debug dylib
to runtime resources; these are not release JARs. No Docker image, toolchain or
release artifact was created. Use existing manual cleanup instructions after
all migration builds finish; nothing is automatically deleted.

## G06 local windows qualification

`${MIGRATION_AUX_ROOT}/20261120_comet12-g06` holds source proposals and snapshots, independent reviews, all failed/passing build/test logs, intentionally failing old-logic controls, existing paired benchmark outputs, verifier controls and the final completion receipt. The complete final log is required to recheck all 88 native/fallback actions. Preserve this evidence through review and final migration acceptance.

Reused `native/target`, `spark/target`, `common/target` and Criterion output directories are listed with whole-directory checkpoint sizes in cleanup-ledger.json. Their total size is not G06-only growth. No Docker image, new toolchain or release JAR was produced. Keep shared caches and prior qualified release artifacts; do not clean build outputs while later groups need them. Only after preserving required evidence and a user cleanup decision may this specific task directory be removed manually. No automatic deletion is authorized.

## G06 accepted integration

`${MIGRATION_AUX_ROOT}/20261120_comet12-g06-integration` contains the qualified
documentation archive, source/ref snapshots, preservation checks, license/static
logs and actual completion receipt. `refs/joom-migration/g06-qualified` retains
the original qualified commit. Preserve this evidence and original G06 logs
through final acceptance; nothing is automatically deleted. Integration creates
no runtime build, release JAR or Docker image. Final auxiliary size and exact
local/remote source identities are in this directory's completion.json.
