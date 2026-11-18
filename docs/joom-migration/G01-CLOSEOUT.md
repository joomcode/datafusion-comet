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

# G01 closeout and RC1 identity

G01 implements the Spark 4.2.1 baseline. The remaining agreed Joom features in G02-G09 have **not** been ported. G02 and Java 21 qualification remain explicitly on hold. This is an internal migration candidate, not the completed production replacement for `joom-1.1-13`.

## Source freeze and release acceptance

This document is committed before the final release build. Its own commit SHA and the future Actions run ID cannot be embedded here without changing the source being qualified. The immutable annotated tag `joom-1.2-1-rc1` is created **only after** exact-source RC qualification succeeds. Its annotation records the final run URL, source SHA and JAR checksums. The downloaded `release-qualification.json`, `commit.txt`, `build-manifest.json`, `sha256.txt` and both smoke receipts provide machine-readable acceptance evidence for those exact bytes. Do not infer successful release acceptance from this document alone or use a historical B93 artifact as RC1.

```bash
git rev-parse joom-1.2-1-rc1^{commit}
git show --no-patch joom-1.2-1-rc1
git merge-base --is-ancestor joom-1.2-1-rc1 branch-1.2
```

Integration uses a fast-forward, preserving the qualified SHA. No post-build documentation or merge commit is added to the release. The final run's artifacts are downloaded to `target/joom-release/gha-<run-id>`. An additional local completion receipt is saved at `/Users/skotlov/d/temp/20261118_comet12-g01-finalization/completion.json`. The Actions run can be located from the tag annotation or [RC Build & Smoke](https://github.com/joomcode/datafusion-comet/actions/workflows/rc_build_smoke.yml).

## Retained behavior and validation

- Exact versions: Comet 1.2.0 base; Spark 4.2.1; Scala 2.13.18; Delta 4.4.1 using `io.delta:delta-spark_4.2_2.13`; DataFusion 55.2.0; native Arrow/Parquet 59.3.2; Java 17; Rust 1.100.0.
- The upstream Delta scanner retains the agreed GCP ADC admission, safe planning, driver-payload serialization and DPP/empty-scan behavior. A conservative encryption-context admission guard preserves fallback.
- The published pristine DataFusion physical-plan import stays a separate commit. G01 changes workspace integration and four IPC statistics test assertions, not vendor production algorithms.
- Spark patch-level decimal casts, mandatory duplicate-Parquet errors and immutable Union/AQE partition-layout handling have targeted regression coverage. Existing Spark SQL fixture assertions are retained with native counterparts.
- Full Comet CI [44205897075](https://github.com/joomcode/datafusion-comet/actions/runs/44205897075), attempt 3, qualified runtime source `26590264b878fb8eae2ca439a8bcc0dfb513cb36`: 127 successful jobs, 17 conditional skips, no failures. Runtime sources, tests, Spark fixture and dependency locks are byte-identical after finalization. This is preserved source-based evidence, not a claim that the rewritten SHA ran full CI again. Automatic full CI is canceled as instructed by the user.
- The final rewritten SHA must independently pass the manual seven-job RC workflow: native AMD64 and ARM64 release builds; one dualarch packaging; 12/12 existing cases on each native architecture (Delta 4, DPP 1, reload 1, ANSI 6); strict receipt binding and artifact export. Canceled or ignored smoke tests are not accepted.
- Loaded bundled native SHA, JVM class origins, actual Spark/Java versions, source/lockfile/graph identity and archive/JAR checksums must agree. Source archive RAT, formatting, CI configuration, Bash syntax and 22 release fault tests must pass.

Four ignored HDFS-cluster tests are inapplicable to Joom GCP; the fifth ignored Rust test is an existing benchmark. Upstream vendor deprecation warnings are documented and were not removed by editing production code. Real GCS/GKE ADC reads, production performance and the remaining feature-group correctness work are not qualified by G01.

## Translation and history preservation

All committed migration documentation is English. User quotations originally written in Russian are translated quotations; the original snapshot is retained in `/Users/skotlov/d/temp/20261118_comet12-g01-finalization/documentation-before.zip`. The full 58 active feature cards plus historical H01, approved nine-group plan and tenth reserve, dependencies, source references, correctness guards and authorization boundaries are preserved. The English text snapshots are `approved-plan.txt` and `correctness-test-plan.txt`; external historical research paths retain their original names.

`G01-HISTORY.json` maps the original G01 commits to the rewritten history. All G01 service/checkpoint documentation is consolidated into one final documentation commit. Eighteen substantive code commits remain; RC implementation and its follow-up fixes are combined. Existing integration-base ancestry is retained. Backup ref `refs/joom-migration/checkpoints/20261118-before-finalization` and the original documentation archive preserve recovery evidence.

The final non-documentation tree must equal pre-finalization source `b93d553856a0339af7468bcff2070e735a1a3c71` byte for byte, including file modes. JSON structures, numbers, booleans, card membership, dependencies and untranslated technical identifiers are checked against the archived originals; changed prose is reviewed for semantic preservation. `MANIFEST.json` covers every document except itself. Final integration verifies the feature, base and tag identities, the qualified release source, checksums and a clean checkout.

Use `README.md`, `STATE.json` and `decisions.json` for continuation. Historical stages and receipts retain their original source-specific conclusions. This closeout and the latest finalization authorization override historical merge/tag holds for G01 only. Do not begin G02 until the user explicitly authorizes it.

For disk accounting and targeted optional cleanup, see `CLEANUP.md` and `cleanup-ledger.json`. Do not delete shared caches, user checkouts or recovery evidence automatically.
