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

> **Current G04 continuation checkpoint, 2026-11-19:** Implementation and local targeted validation are complete; see G04-report.md/G04-results.json and `${MIGRATION_AUX_ROOT}/20261119_comet12-g04/completion.json`. Memory, Partial metadata and ordering failures were corrected; regressions pass without higher budgets or weaker checks. Await user review: no push, merge, full GHA, G05 or protected-ref changes. Preserve historical checkpoints below.

> **Final source-freeze note:** G01 implementation and documentation are complete. RC1 acceptance is a post-commit gate recorded by the immutable `joom-1.2-1-rc1` tag and the delivered artifact receipts; see `G01-CLOSEOUT.md`. Earlier source SHAs, runs and pending states below are historical evidence. G02 and Java 21 work remain unauthorized.

> **G01 finalization authorized:** documentation and history cleanup, followed by exact-SHA RC qualification, are in progress. Integration into `branch-1.2` and `joom-1.2-1-rc1` are conditional on successful checks. G02 and Java 21 work remain on hold.

# G01 continuation checkpoint

Latest completed qualification: **RC Build & Smoke 44388252264 on b93d553856a0339af7468bcff2070e735a1a3c71 succeeded. Final JARs were downloaded and locally verified.** All 7 jobs and 12/12 packaged tests on AMD64 and ARM64 passed. Directory: `target/joom-release/gha-44388252264`. Apache RAT for source b93 passed; runtime code has not changed since full CI on 26590264b. Monitoring and builds ended. Final progress documents were saved locally without another push. The user has now authorized G01 finalization, English documentation, service-history cleanup, an exact-SHA RC rerun as needed, integration and RC1 tagging after successful checks. Earlier merge/tag holds below are historical. Publication, G02 and Java 21 work remain excluded.

- Feature `joom/1.2-dev`; PR https://github.com/joomcode/datafusion-comet/pull/28. The abbreviated historical base was 9691385f. The pre-integration base `branch-1.2` is `9691385f35e4c5bd29514dca78d2044dd20e4bed`. Verify actual refs and the current finalization state before resuming.
- Earlier run44180095887/ac3694504 ended in FAILURE:124success/3failure/17skipped. All nine original failed jobs passed; two additional cases were discovered and have since been fixed in the source above. Do not attribute an old verdict to a new source.
- Source35/Scala2.12 controls35passed;41union12passed;42union12passed; reader27passed/converter19passed; fresh-JAR SparkSQL21+9+3passed. The pre-fix negative control on41 did return incorrect groups. All source files after commit matched the tested worktree byte for byte.
- Run44205897075 completed successfully on its exact HEAD; polling ended. New auxiliary directory: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-ci-fixes/20261117_followup-gha`; check-pr-gha.py/fetch-job-log.py are available there. Helper g01_gha_monitor_five_minutes finished and all final receipts were saved; do not overwrite immutable receipts of the previous run.
- Older Linux release JARs use source2b8f80b3f and **do not contain the fixes**. Do not treat them as a final RC or production-ready. Host/M2 JARs with the new Scala code were used only for local tests. Dualarch packaged smoke remains an RC gate; it passed for the newer b93 artifact set.
- Explicitly select Java17; use direct stable-toolchain Cargo bin and JNI-library PATH when needed. Working official protoc33.5 is already in the ProtocBridge cache. Global SDK/PATH/host tools were not changed.
- All execution documentation is here; preserve caches/user checkouts. New test-only/network outputs are in the cleanup ledger. Git feature push/PR28 updates are authorized; artifact publication/production operations are excluded.
