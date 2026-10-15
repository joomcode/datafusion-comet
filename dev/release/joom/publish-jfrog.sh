#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: dev/release/joom/publish-jfrog.sh <version> <out-dir>

Publishes the jars built by build-release.sh as org.apache.datafusion:<artifact>:<version>
to jfrog joom-maven-local. The version is the git tag, e.g. joom-1.1-10; <out-dir>/commit.txt
must be the commit of that tag. Refuses to overwrite an existing version.

Environment:
  JFROG_AUTH   base64 user:token for https://jfrog.joom.it (the "auth" of jfrog.joom.it in ~/.docker/config.json)
USAGE
  exit 2
}

[ $# -eq 2 ] || usage
V=$1
OUT="$(cd "$2" && pwd)"
: "${JFROG_AUTH:?JFROG_AUTH is not set}"
H="Authorization: Basic $JFROG_AUTH"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
C="$(cat "$OUT/commit.txt")"
test "$(git -C "$REPO" rev-parse "$V^{commit}")" = "$C"
(cd "$OUT" && shasum -a 256 -c sha256.txt)
API=https://jfrog.joom.it/artifactory/api/storage/joom-maven-local/org/apache/datafusion
BASE=https://jfrog.joom.it/artifactory/joom-maven-local/org/apache/datafusion
GET=https://jfrog.joom.it/artifactory/joom-maven/org/apache/datafusion
W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT

for A in comet-spark-spark3.5_2.12 comet-contrib-delta-spark3.5_2.12; do
  code=$(curl -s -o /dev/null -w "%{http_code}" -H "$H" "$API/$A/$V")
  [ "$code" = 404 ] || { echo "$A $V already exists ($code)" >&2; exit 1; }
done

for A in comet-spark-spark3.5_2.12 comet-contrib-delta-spark3.5_2.12; do
  cat > "$W/$A-$V.pom" <<POM
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.apache.datafusion</groupId>
  <artifactId>$A</artifactId>
  <version>$V</version>
  <packaging>jar</packaging>
  <description>Joom build of Apache DataFusion Comet 1.1.0, git tag $V ($C) in github.com/joomcode/datafusion-comet</description>
</project>
POM
  cp "$OUT/$A-1.1.0.jar" "$W/$A-$V.jar"
  for f in "$A-$V.jar" "$A-$V.pom"; do
    s1=$(shasum -a 1 "$W/$f" | cut -d' ' -f1)
    s256=$(shasum -a 256 "$W/$f" | cut -d' ' -f1)
    m5=$(openssl md5 -r "$W/$f" | cut -d' ' -f1)
    curl -sf -H "$H" -H "X-Checksum-Sha1: $s1" -H "X-Checksum-Sha256: $s256" -H "X-Checksum-Md5: $m5" \
      -T "$W/$f" "$BASE/$A/$V/$f" -o /dev/null -w "$f %{http_code}\n"
  done
  curl -sf -o "$W/dl.jar" "$GET/$A/$V/$A-$V.jar"
  cmp "$W/dl.jar" "$OUT/$A-1.1.0.jar"
  echo "$A $V: download identical"
done
