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

> **Final source-freeze note:** G01 implementation and documentation are complete. RC1 acceptance is a post-commit gate recorded by the immutable `joom-1.2-1-rc1` tag and the delivered artifact receipts; see `G01-CLOSEOUT.md`. Earlier source SHAs, runs and pending states below are historical evidence. G02 and Java 21 work remain unauthorized.

> **G01 finalization authorized:** documentation and history cleanup, followed by exact-SHA RC qualification, are in progress. Integration into `branch-1.2` and `joom-1.2-1-rc1` are conditional on successful checks. G02 and Java 21 work remain on hold.

# RC Build & Smoke

Separate manual workflow: `.github/workflows/rc_build_smoke.yml`, display name **RC Build & Smoke**. It does not trigger Comet CI or publish to JFrog/Nexus/production. The entire build runs in GitHub Actions, without transferring JARs from the laptop.

## Running it

In Actions, select RC Build & Smoke → Run workflow → the desired branch/tag. Set `expected_commit` to the full lowercase Git SHA of that ref. It must match the workflow ref SHA; another source will be rejected before builds. The new file is already registered by bootstrap commit `fa7347e5ea549a01c77e325dbd3449d93c9d5700` on default `main`; it was the only file changed there. G01 sources were not moved into main.

Previously qualified run: https://github.com/joomcode/datafusion-comet/actions/runs/44388252264, source `b93d553856a0339af7468bcff2070e735a1a3c71`. All 7 jobs passed, including 12/12 existing smoke cases on each architecture. History cleanup changes source identity; the final RC requires matching exact-SHA qualification.

## Stages and acceptance criteria

1. Source gate: exact SHA/cleanliness/lockfile and 22 fast tooling tests.
2. Native matrix: Linux AMD64 and ARM64 on native runners; Cargo `--locked --release`, Rust1.100.0, Ubuntu20.04, GCC10, JDK17. AMD64 flags `-Ctarget-cpu=x86-64-v3`; ARM64 baseline flags are empty. Commit/lock/compiler/image/ELF/SHA256 are recorded.
3. Package: validate both native provenance records; one Maven reactor `package dependency:build-classpath` for Spark4.2.1/Scala2.13.18/Delta4.4.1. Produce exactly two runtime JARs with both `.so` files, matching portable test support and manifest/checksums. Use the same release script/validator as local builds.
4. Smoke matrix: download the same candidate without Maven/rebuilding; run Delta4+DPP1+reload1+ANSI6. All12 must pass with zero failed/canceled/ignored. Verify actual Spark4.2.1, Java17, native architecture, final-JAR class origins and SHA256 of the bundled `.so` actually loaded.
5. Final: both receipts must refer to the same JAR/source/lock/package/support hashes. Only then produce a qualified artifact. JARs are not repackaged after smoke.

The strict binding of receipts to bytes prevents reuse of an older successful smoke after replacing a package artifact during retry. Partial native/candidate artifacts are not a qualified release.

Focal Git2.25.1 ignores command-scope `safe.directory`: the release builder mounts a separate staging configuration trusting only the container checkout path at `/etc/gitconfig:ro`. Host Git configuration is unchanged. Maven classpaths are computed in the same reactor so a clean runner does not need a prior install of internal Comet modules.

## Versions and use

| Component            | Version                                            |
| -------------------- | -------------------------------------------------- |
| Spark / Scala binary | 4.2.1 / 2.13                                       |
| Scala / JDK          | 2.13.18 / 17                                       |
| Comet upstream base  | 1.2.0, pinned branch-1.2 source93cc6f9d            |
| Delta                | 4.4.1, `io.delta:delta-spark_4.2_2.13:4.4.1`       |
| DataFusion           | 55.2.0, local physical-plan vendor                 |
| Arrow/Parquet native | 59.3.2                                             |
| Arrow/Parquet JVM    | 18.3.0 / 1.17.0                                    |
| Platforms            | Linux AMD64(x86-64-v3), ARM64; GLIBC imports<=2.31 |

Runtime files: `comet-spark-spark4.2_2.13-1.2.0.jar` and `comet-contrib-delta-spark4.2_2.13-1.2.0.jar`. The second is an adapter: the official Delta Spark4.2 artifact4.4.1 and its dependencies are required separately. DataFusion/native Arrow/Parquet are embedded in `.so`.

Future Maven coordinates: `org.apache.datafusion:comet-spark-spark4.2_2.13:<joom_version>` and `org.apache.datafusion:comet-contrib-delta-spark4.2_2.13:<joom_version>`. Planned RC1 `joom-1.2-1-rc1`, stable `joom-1.2-1`; this workflow creates neither tags nor publications. The G01 candidate does not yet include the later G02–G09 ports.

## Artifacts and retention

Qualified artifact: `rc-build-smoke-<fullSHA>`; previously qualified ID14469302529. It contains runtime JARs, source/lock/toolchain/native metadata, checksums, both smoke receipts/logs and `release-qualification.json`. Native intermediate artifacts are retained for3 days, candidate with test support for7 days, qualified/smoke for14 days. Caches are restore-only; feature runs do not write large cache entries.

After downloading, verify the artifact-archive SHA256, `sha256.txt`, embeddedGit/version/ELF/provenance and binding of both receipts to final JARs. The local result of the previously qualified run is `target/joom-release/gha-44388252264`. Removal and sizes are documented in `CLEANUP.md`; preserve shared caches and user checkouts.

Full Comet CI for release-only commits was canceled at the user's direct instruction. Runtime/native/JVM code matches qualified 26590264b (CometCI44205897075:127success/17conditional skips); the new workflow additionally checked the release profile and final JARs. Source-tarball Apache RAT for b93d55385 passed separately. This does not constitute production GCP/performance qualification. G01 integration/tagging has subsequently been authorized conditional on finalization checks; G02 remains separately gated.
