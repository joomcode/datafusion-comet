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

# Network sources and approval boundaries

The user approved the grouped dependency operations below for the Comet 1.2 /
Spark 4.2.1 migration and its qualification, including necessary child processes
inside build containers. The condition is that downloaded/executed artifacts are
official, known and widely used, with no reasonable safety concerns. This records
the existing scope; it does not authorize unrelated future work or new sources.
Current task instructions and the active sandbox/network policy still apply.

| Purpose                                                                                 | Operation families                                                                       | Sources                                                                                                                                                         |
| --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| JVM dependencies, build/test plugins and Spark SQL build                                | Maven wrapper, Maven/SBT resolution/build/test/package/install                           | repo.maven.apache.org, repo1.maven.org, repo.scala-sbt.org, repo.typesafe.com and normal redirects                                                              |
| Rust compiler/components and crates                                                     | rustup and locked Cargo fetch/build/test/bench/fmt/clippy                                | sh.rustup.rs, static.rust-lang.org, index.crates.io, static.crates.io, crates.io                                                                                |
| Pinned Cargo Git dependencies, required official build tools, read-only GitHub evidence | Locked Git dependency resolution, Protobuf/tool downloads, GitHub API/artifact downloads | github.com, api.github.com, raw.githubusercontent.com, codeload.github.com, release-assets.githubusercontent.com, objects.githubusercontent.com and normal CDNs |
| Linux builder/base images                                                               | Docker pull/build/buildx/run                                                             | registry-1.docker.io, auth.docker.io, production.cloudflare.docker.com and standard Docker blob/CDN endpoints                                                   |
| Ubuntu packages inside builder containers                                               | apt package installation/update                                                          | archive.ubuntu.com, security.ubuntu.com, ports.ubuntu.com and standard mirrors                                                                                  |
| Exact Temurin on GitHub runners                                                         | Official actions/setup-java download                                                     | api.adoptium.net, official Adoptium GitHub releases and normal redirects/CDNs                                                                                   |
| Locked Spark 4.2 Python prerequisites in GHA                                            | pip --require-hashes using dev/ci/requirements-spark42-python.txt                        | pypi.org, files.pythonhosted.org                                                                                                                                |

The Java 25 extension allowed local Python metadata/SHA256 reads but no local
Python package installation. The existing user-installed Temurin SDK is reused
locally. The user additionally authorized the necessary quality/qualification
setup inside GHA/runners; this is not permission to install arbitrary host tools.
Use the exact pinned dependencies and publisher checksums, and inspect downloaded
scripts before execution. Lockfile inclusion alone does not satisfy the safety
condition. Ask separately about unusual tools, private registries or new sources.
Keep authentication restricted to its intended GitHub origin; never forward
GitHub credentials to redirected artifact/blob hosts. Do not print secrets.

The user provided research checkouts; request missing source checkouts instead of
obtaining source through a UI or silently cloning arbitrary projects. Respect the
separate datasci prohibition. Inspect benchmark scripts for hidden clone/install/
publish operations before running them.

User-authorized feature pushes, workflow control and integrations were scoped to
their requested stages. They are not standing authority to publish a new release.
Stable tags, Maven/JFrog deploy, Docker push, production jobs/queries/writes,
datasci changes, new main commits, host Homebrew/SDKMAN installation, privileged
system setup and shared-cache cleanup remain separate actions requiring the
relevant user instruction. Source changes do not implicitly authorize CI runs.

Save chosen logs, downloads and one-off scripts under the approved dated auxiliary
directories. Maven/Cargo/Rustup/SBT/Docker may use their standard caches. Ask for a
missing permission or resource; do not change cache roots, install duplicate
environments or leave the sandbox as an improvised workaround.
