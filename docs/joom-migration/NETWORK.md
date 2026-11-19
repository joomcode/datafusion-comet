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

# Approved network scope

Status: APPROVED_WITH_USER_SAFETY_CONDITION, 2026-11-17.
Explicit consent and its condition are recorded in decisions.json.

The user gave one authorization for the entire approved G01-G09 migration and
necessary G10 qualification through delivery of local JARs, including repeated
builds and tests. It covers the listed downloads and necessary child-process
requests on the host and inside Docker, not just one literal command.

| Purpose                                                                        | Command families                                                                  | Expected hosts                                                                                                                                                                                                                                        |
| ------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Maven wrapper/JVM dependencies/plugins, Spark test build                       | `./mvnw ...`, Maven compile/test/verify/package/install and Spark `build/sbt ...` | `repo.maven.apache.org`, `repo1.maven.org`; if necessary `repo.scala-sbt.org`, `repo.typesafe.com` and their normal redirects                                                                                                                         |
| Rust compiler/components and crates                                            | `rustup toolchain/component ...`, `cargo fetch/build/test/bench/fmt/clippy ...`   | `sh.rustup.rs`, `static.rust-lang.org`, `index.crates.io`, `static.crates.io`, `crates.io`                                                                                                                                                            |
| Pinned Cargo Git dependencies and binary build tools                           | Cargo fetch/build, Protobuf release binary download; read-only GitHub API         | `github.com` (in particular `apache/iceberg-rust` at the pinned commit), `api.github.com`, `raw.githubusercontent.com`, `codeload.github.com`, `release-assets.githubusercontent.com`, `objects.githubusercontent.com` and their normal CDN redirects |
| Linux builder images and build/run with the listed downloads inside containers | `docker pull/build/buildx build/run ...`                                          | Docker Hub: `registry-1.docker.io`, `auth.docker.io`, `production.cloudflare.docker.com` and standard blob/CDN endpoints                                                                                                                              |
| Ubuntu build packages inside the builder                                       | `apt-get update/install ...`                                                      | `archive.ubuntu.com`, `security.ubuntu.com`, `ports.ubuntu.com` and standard Ubuntu mirrors/redirects                                                                                                                                                 |

Authorization includes dependency-resolution requests, downloads of archives,
metadata/checksums/signatures, and public build tools and fixtures that are
dependencies of the selected build/tests. Preserve pinned dependencies and
Cargo.lock; arbitrary dependency upgrades are outside this permission.
Save results, logs and executor-downloaded auxiliary files in the authorized
locations; normal Maven/Cargo/Rustup/Docker caches may use standard paths.

Examples of expected commands after the necessary files are prepared (select for
the current stage; network authorization is not an instruction to run all examples):

```sh
./mvnw -B -Pspark-4.2,delta -Dspark.version=4.2.1 -Ddelta.spark.version=4.4.1 verify
cargo fetch --locked --manifest-path native/Cargo.toml
cargo test --locked --manifest-path native/Cargo.toml -p datafusion-comet-spark-expr
rustup toolchain install 1.99.0 --profile minimal
docker pull --platform linux/amd64 ubuntu:20.04
docker pull --platform linux/arm64 ubuntu:20.04
```

Refine exact flags/test package selection from the sources and current stage;
examples do not replace the mandatory native-before-JVM build order. Separate
vendor unit tests must use the same workspace/lock/patch graph as the release.
The current wrapper pins Maven3.9.6. The old Joom Linux builder pins Rust1.99.0,
Ubuntu20.04/GCC10/Java17/Protobuf30.2; adapting it is part of G01. These are historical
baseline details, not permission to override the versions recorded in final manifests.

The original scope excludes Git push/PR publication, Maven deploy/JFrog publish,
Docker push, production GCP queries/jobs/writes, Spark image changes, messages to
people, host Homebrew/SDKMAN installations, privileged binfmt/system setup, shared
cache cleanup and arbitrary new source clones. Ask the user for a missing research
checkout. If another private registry or other source is actually needed, identify
it and obtain a scope extension first. Later scoped Git permissions are below.

The user's explicit consent (English translation from Russian):

> I authorize, for the entire Comet1.2/Spark4.2.1 migration, the network operations listed in docs/joom-migration/NETWORK.md: Maven/SBT, Rustup/Cargo, pinned Git dependencies and build tools from GitHub, Docker Hub and Ubuntu packages, including child processes inside containers. This is subject to the condition that everything downloaded from the network to my laptop and executed is a known and widely used artifact whose safety for my machine is not reasonably in doubt. No push, external publication or production operations.

Execution condition: official sources, known widely used artifacts, pinned
versions/commits and available publisher checksums; inspect external installer/build
scripts before execution. Registry/lockfile inclusion does not override the user's
safety condition. Obtain separate agreement for unusual or questionable tools/sources.
This translation does not expand the original list of operations or hosts.

## Scoped addition: G01 PR preparation, 2026-11-17

That user request authorized preparing/pushing the G01 branch to
github.com/joomcode/datafusion-comet and providing a manual PR handoff. An absent
base branch could be published only at its exact unchanged SHA; existing base
contents/ref could not be changed. This was an exception to the historical no-push
clause above, not authorization for merge, RC1/G02, artifact publication or production.
Read-only GitHub checks/runs for the PR were in scope. Expected hosts:
github.com/api.github.com and standard GitHub redirects. The user would create the
PR manually; gh installation was not required.

## Scoped addition: G01 finalization, 2026-11-18

The latest explicit instruction authorizes G01 history cleanup, updating its PR,
repeating RC Build & Smoke for the resulting source when necessary, integration
into branch-1.2 and creation of joom-1.2-1-rc1 after successful verification, followed
by completeness checks. It supersedes the earlier G01 no-merge/no-RC1 hold.
The already authorized GitHub operations include workflow control/status/logs and
artifact download. This does not authorize JFrog/Maven/Docker artifact publication,
production operations, further Java 21 work or G02-G10 implementation.

## Repository/CI boundary update, 2026-11-19

No migration work may access ${DATASCI_ROOT}/; another chat
works there in parallel. Further CI changes belong in authorized Comet feature
branches and branch-1.2 only; no additional main commits. The earlier G01 main
bootstrap permission was specific and historical. The new testing schedule in
decisions.json#future_migration_validation_policy does not expand approved
network hosts, artifact publication or production permissions. Each next G
still requires separate explicit user authorization.

## G02 feature push authorization (2026-11-19)

The user approved `git push -u origin joom/1.2-native-runtime` to github.com/joomcode/datafusion-comet and the necessary read-only remote-ref verification, after local validation and folding the follow-up fix. This does not authorize branch-1.2/main/tag updates, creating a PR that launches full CI, artifact publication or production operations. The final source/remote SHA receipt stays under the G02 follow-up auxiliary directory.

## G02 integration authorization (2026-11-19)

The user's explicit G02 merge instruction supersedes the earlier branch-1.2 hold for this stage. Necessary Git operations use github.com/joomcode/datafusion-comet: inspect remote refs/feature PR metadata, update the consolidated feature with an exact force-with-lease if its published commit is rewritten, and publish branch-1.2 by fast-forward. Prefer an atomic two-ref push; never overwrite an unexpected remote update. No main or tag push, PR/CI launch, artifact publication or production operation is authorized by this step. Existing read-only GitHub API and official Apache RAT/Maven download scopes still apply.
