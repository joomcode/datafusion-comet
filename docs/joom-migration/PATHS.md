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

# Portable paths and local evidence

Committed project files, including migration documentation, must not contain a
developer's absolute checkout, home, auxiliary or temporary-directory paths.
Use repository-relative paths for Comet files and build outputs, and the named
roots below for resources outside the checkout. Standard container/system paths
and intentional absolute-path test fixtures retain their existing meaning.

## Resolving the notation

| Notation                                              | Meaning                                                                                                                                                                                                                                 |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `docs/joom-migration/...`, `native/...`, `target/...` | Relative to the current Comet repository root, not to the document directory or the shell's arbitrary current directory.                                                                                                                |
| `${COMET_ROOT}`                                       | Explicitly selected current Joom Comet checkout, including its `.git` when authorized.                                                                                                                                                  |
| `${MIGRATION_AUX_ROOT}`                               | User-approved auxiliary root containing the existing dated task directories. Keep their names and contents unchanged. New task directories follow `YYYYMMDD_description`; existing user-supplied `src` checkouts remain shared sources. |
| `${UPSTREAM_COMET_ROOT}`                              | User-provided Apache Comet research checkout.                                                                                                                                                                                           |
| `${UPSTREAM_DATAFUSION_ROOT}`                         | User-provided Apache DataFusion research checkout.                                                                                                                                                                                      |
| `${UPSTREAM_SPARK_ROOT}`                              | User-provided upstream Spark research checkout.                                                                                                                                                                                         |
| `${JOOM_SPARK_ROOT}`                                  | User-provided existing Joom Spark checkout.                                                                                                                                                                                             |
| `${DATASCI_ROOT}`                                     | The separate datasci repository. **Never access it for this migration, at this or any other checkout location.** This name describes a prohibition, not an approved resource.                                                           |
| `${HOME}`                                             | The relevant user's home directory for standard caches, SDKMAN and the Docker Desktop socket; an example does not prescribe another person's home.                                                                                      |
| `${RUSTUP_PACKAGE_ROOT}`                              | The installed Homebrew rustup package prefix. Obtain the local value from `brew --prefix rustup` when needed; do not change the user's installation.                                                                                    |
| `${TMPDIR}`                                           | The tool-managed temporary root in the illustrative upstream debug log. This does not change the approved location for executor-selected auxiliary files.                                                                               |

These are **literal documentation placeholders** in JSON and text. JSON does not
expand environment variables. A reader or tool must supply the explicit local
bindings and resolve only the documented names; never use `eval` on a document.
Run repository-relative command examples from the selected Comet root. Shell
examples that use variables must quote them and reject unset/empty required roots.
For cleanup, validate the selected checkout and retain the exact narrow targets;
the notation does not authorize deletion or broaden any permission.

Actual local bindings belong outside Git. For this conversion they are preserved
in `${MIGRATION_AUX_ROOT}/20261119_comet12-portable-paths/local-paths.json`.
Use the current user-provided checkout and auxiliary-root locations to find that
file when resuming; verify the bindings before use. Do not infer or silently
substitute a different root when a binding is missing. Request the missing
location instead. Root names are a portable description of existing permissions,
not new filesystem, network, Git or production authorization.

## Historical evidence and preservation

The 2026-11-19 conversion changes path notation only. Historical committed command
arrays, stdout, size listings, translated user statements and snapshots now use
the same normalized notation. They are readable records of the original events,
not byte-identical copies of raw output. Dates, measurements, source/artifact
hashes, run IDs, test outcomes, permissions, feature cards and stage gates retain
their meaning. Existing qualification receipts still identify their original
source commit; a documentation-only commit does not requalify release artifacts.

No referenced evidence, checkout, artifact or cache is renamed, moved or deleted.
Original receipts remain at the same local locations. Exact pre-conversion
documentation is preserved in
`${MIGRATION_AUX_ROOT}/20261119_comet12-portable-paths/documentation-before.zip`
(SHA256 `717c8a7d30b4ccb401c2fe83b186f94ee57efecf7e872408dac676861824a776`).
The same directory contains the local bindings, replacement rules, reference
audit and post-commit completion receipt. Preserve it until migration acceptance;
do not commit the local bindings or automatically delete these records.

`MANIFEST.json` covers the current documentation bytes and is regenerated after
intentional edits. Historical source/artifact hash fields and immutable Git tags
are not rewritten. G02 remains complete; G03 and later groups still require
separate explicit user authorization.
