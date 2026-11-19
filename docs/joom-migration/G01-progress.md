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

# G01 implementation history

The entries below are historical checkpoints, not current authorization. See `STATE.json`, `G01-report.md` and `decisions.json` for the latest state and decisions.

> **Historical PR preparation:** history aligned without changing the tree/24 documents or branch-1.2; three ANSI guards removed for actual AMD64 GHA execution. Local checks were fast, full scope was delegated to GHA. The user creates the PR using G01-PR.md. No merge/RC1/G02 was authorized then.

> **Historical status:** G01 implemented; AMD64 runtime qualification incomplete. `branch-1.2` unchanged; user preview/confirmation required first. G02 unauthorized. Results: G01-report.md / G01-results.json.

> **Pause on 2026-11-17:** all our builds/tests stopped at user request. Code saved in Git; G01 incomplete. Authoritative continuation point: `PAUSED.md`; no restart without explicit resume. G02 unauthorized.

**G01 resumed on2026-11-17; new sandbox filesystem/TCP/Docker-socket probes passed. Historical checkpoint: PAUSED.md; current status: STATE.json.**

Status at the initial2026-11-17 checkpoint: registry baseline passed; G01.2 pristine vendoring starting. G02–G10 prohibited pending separate user authorization. Main executor: coordinator chat. Branch joom/1.2-dev created from branch-1.2; upstream rechecked through GitHub API:93cc6f9d71136bd0d87f77dd901989144d8c09fb. Registry baseline committed as 71435d6f1; pristine vendor as 98c9f2f19. Unfinished integration/release changes preserved in dirty tree and checkpoint.

## Completed at that checkpoint

- Spark4.2.1/Scala2.13.18/Java17, Delta delta-spark_4.2_2.13:4.4.1 in the profile.
- Maven wrapper3.9.6 works; effective POM confirmed. Log and XML in the task directory.
- Native registry build passed: cargo build --locked -j4 -p datafusion-comet, preserving native/Cargo.lock. DF55.2.0/Arrow-Parquet59.3.2; Mac arm64 libcomet.dylib created in native/target/debug. Duration4m07; a compilation check, not Rust unit tests.
- ADC whitelist and two original Joom tests ported from a43523692.
- Delta partitioning does not invoke file planning/subqueries; core obtains actual execution count from perPartitionData. Added scalar DATA-filter metadata regression.
- Delta nativeOp/serializedPlanOpt transient; splits test reads actual data. Full query-text interning and its regression belong to G08/J17 and are not claimed here.
- Added zero-partition pruning regression from 45fad9eea (production equivalent already upstream).
- CI Spark4.2.1 + Delta4.2 lane/routing; python3 dev/ci/check-ci-config.py passed.
- Removed -pl from touched Makefile/Delta-workflow paths.
- Local Spark fixture created WITHOUT network from the user's checkout in target/joom-spark-4.2.1. Original ${UPSTREAM_SPARK_ROOT} unchanged. Diff4.2.0 applied three-way to v4.2.1; six import-only conflicts resolved, preserving new Spark imports and Comet imports. dev/diffs/4.2.1.diff generated from an actual Spark Git diff,283345bytes; clean-index git apply --check passed.

## Registry-baseline results

- Local RPC environment corrected using standard SPARK_LOCAL_IP=127.0.0.1. DPP/NativeBase smoke:10/10 + ParquetEncryptionITCase, BUILD SUCCESS.
- First full Delta run:283/284. ADC, scanner, deletion vectors, DML, planning-deadlock and driver-payload serialization regressions passed.
- Sole failure: Hadoop4.2 added fs.s3a.encryption.context, correctly detected by the discovery harness without an admission guard.
- Adapted the production+unit portion of upstream draftPR7593 (not the whole PR): reject native execution for nonempty/unverifiable Hadoop-effective S3 context, including sidecar DV with a local Delta log; aliases/JCEKS/redaction retained. No AWS network use; GCS/file bypass without provider read. DeltaScanContribSuite:164/164 passed, BUILD SUCCESS1m56s. An explicit gs:// control remained to be checked on the next Delta compile.
- ANSI baseline52647 BUILD SUCCESS:65 passed,3 standard upstream cancellations of old generic arithmetic tests on Spark4.2 (issue4967); not full ANSI proof. Existing CometExpressionSuite ANSI, CometMathExpressionSuite and SQL fixtures ansi/case_when_lazy checked. A nonexistent CometAnsiExpressionSuite was NOT used as evidence.

Logs of all successful and failed runs are preserved in the task directory below. The first failed run selected suites in the wrong Maven modules; the next failed on local RPC; both were diagnosed and fixed. Maven/Cargo used standard caches through then-approved escalated executions. Persistent sandbox permissions were unchanged. ps was unavailable; no workaround was attempted.

## Planned next steps at that checkpoint

1. Complete the ANSI registry baseline and fix G01 compilation/test failures. Do not hide new failures or claim unrun suites passed.
2. Only after the registry baseline: import pristine published physical-plan55.2 in a separate mechanical commit, then root patch/workspace/shared lock/test target. Cargo already downloaded the archive to ~/.cargo; verify checksum b1451d6817e5ac0dd21819b19949d1c420ab51d748f55dd9a96ecb5418a24c97.
3. Repeat Comet/Delta/ANSI/native/vendor checks and SparkSQL4.2.1 suites.
4. Adapt release builder/publisher, locked graph, GLIBC<=2.31, both architectures, local validate-only publisher checks, actual smoke tests. RC1 is internal and an incomplete port. No publication or production operations.
5. Actual GCP ADC reads require an authorized isolated test resource; current network scope does not permit productionGCP. An auth-gate unit test does not constitute such a read.

Main logs/download directory: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01`. Add target/joom-spark-4.2.1, native/target and actual Maven/Cargo caches to the cleanup ledger. When changing sessions, first check actual Git status/HEAD and active tool sessions.

## At the pause

Registry baseline and pristine vendor committed. Workspace integration and release scripts still dirty/unqualified. Native core961pass/12UDF-path fail/5ignored; vendor unit tests not run. All commands ended. See PAUSED.md.

## After resuming

- New sandbox roots, local TCP and Docker socket checked successfully without escalation.
- Cargo1.100 OUT_DIR lookup corrected;12 UDF failures resolved.
- Workspace unit tests:4326 passed,5 ignored; vendor1695/1695.
- Four vendor test-only spill-size assumptions replaced by comparison with the physical flushed/finalized file size; production algorithms unchanged. Cause: Arrow59.3.2 omits all-valid bitmaps, whereas the published vendor lock used59.2.0.
- Strict Clippy for all Comet crates passed; pristine vendor excluded from lint because Rust1.100 deprecates fetch_update. Its unit gate retained.
  -19 negative/model release-validator/publisher checks passed using mock HTTP/Git.
- Linux arm64 builder created; amd64 builder in progress. Release cargo builds not yet started.
- Vendor-linked root Delta install:291/291 PASSED8m20s, including gs control.
- Vendor-linked ANSI/runtime:75passed,3upstreamcanceled, BUILD SUCCESS1m40s.
- SparkSQL dependency setup: POM-only artifact holes filled selectively with standard Maven; shared cache neither moved nor cleared. Installed-fork SHA recorded.
- Both Linux builder images prepared; amd64 CPUID confirmsx86-64-v3.
- Release-script review closed: fresh module outputs, actual GitSHA, SPI providers, actual graph/toolchain/ELF imports, PUT2xx, optimizer-safe cleanup guards. Dirty-source refusal checked before Docker/output creation.
- I03 clarified against core/Spark4.2.1: Unknown0 matches the contract; a possible extra exchange versus obsolete contribUnknown1 was not measured.
- Next major run was the release builder; source/HEAD must remain fixed while it runs. Save live process IDs/statuses in external active-commands.json.

## Resuming after the second pause

Checked inside sandbox: ProtocBridge cache write, loopback bind/connect, Docker27.4.0. The user confirmed both architectures must be completed in G01 and service checkpoint commits combined. Implementation/checks only G01; current raw outputs: ${MIGRATION_AUX_ROOT}/20261117_comet12-g01-final-qualification. Service history is combined before the final build, preserving substantive commits; mapping and backup ref are in this task directory.

## PR28 runner allocation fix

GitHubAPI confirmed Preflight/RequiredChecks had ubuntu-slim labels, empty runner_name,0 steps. Replace6 runs-on labels in ci.yml/PRtitle/Icebergcoverage with ubuntu-24.04; update sourceCI diagram/comments. Tests/commands unchanged. LocalCIconfigcheckpassed. RAT found4migrationdocumentswithoutheaders; standardheadersadded. pull_request_target labeler reads unchangedbase and remains outsidefeaturefix. Rawlogs: ${MIGRATION_AUX_ROOT}/20261117_comet12-g01-runner-fix.

## PR28 Markdown preflight fix

Runner replacement confirmed by actual allocated ubuntu-24.04 runner in run44135599020. RAT and separate PR title check passed; Preflight stopped at four migration Markdown files. User installed Homebrew Prettier3.10.1. Only the four flagged files were formatted; all repository Markdown now passes. Text preserved after ignoring whitespace/table separator alignment; receipt is in the runner-fix task directory. No runtime/test coverage/base branch changes. Push and new GHA verdict pending at that historical checkpoint.
