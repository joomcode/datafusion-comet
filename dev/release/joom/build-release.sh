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
Usage: dev/release/joom/build-release.sh <out-dir>

Builds the Joom release jars of Comet for Spark 3.5 / Scala 2.12 from HEAD:
libcomet.so for linux/aarch64 and linux/amd64, each in an Ubuntu 20.04
container (dev/release/joom/Dockerfile), so that the libraries need at most
GLIBC_2.31, then comet-spark and comet-contrib-delta jars with both libraries.

Environment:
  DOCKER_CONTEXT      docker context to build in (needs linux/arm64 and linux/amd64)
  M2_DIR              host Maven repository mounted into the jar build (default ~/.m2)
  MVN_OFFLINE=1       build the jars with mvnw -o
  CARGO_JOBS          cargo -j (default 8)
  TARGET_DIR          host directory for the cargo target dirs (default <out-dir>/cargo-target)
  CARGO_REGISTRY      prefix of the docker volumes that cache the cargo registry per arch (default comet-cargo-)
  MAX_GLIBC           highest GLIBC symbol version allowed (default 2.31)
USAGE
  exit 2
}

[ $# -eq 1 ] || usage
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT="$(mkdir -p "$1" && cd "$1" && pwd)"
M2_DIR="${M2_DIR:-$HOME/.m2}"
CARGO_JOBS="${CARGO_JOBS:-8}"
TARGET_DIR="${TARGET_DIR:-$OUT/cargo-target}"
MAX_GLIBC="${MAX_GLIBC:-2.31}"
CARGO_REGISTRY="${CARGO_REGISTRY:-comet-cargo-}"
IMAGE=comet-joom-builder-focal
JARNAME=comet-spark-spark3.5_2.12-1.1.0.jar
DELTANAME=comet-contrib-delta-spark3.5_2.12-1.1.0.jar
GITDIR="$(cd "$REPO" && cd "$(git rev-parse --git-common-dir)" && pwd)"

test -z "$(git -C "$REPO" status --porcelain --untracked-files=no)" || { echo "uncommitted changes in $REPO" >&2; exit 1; }
COMMIT="$(git -C "$REPO" rev-parse HEAD)"
mkdir -p "$OUT/linux/aarch64" "$OUT/linux/amd64" "$TARGET_DIR/arm64" "$TARGET_DIR/amd64"

for a in arm64 amd64; do
  docker build --platform "linux/$a" -t "$IMAGE:$a" "$REPO/dev/release/joom"
done

run() {
  local a=$1; shift
  docker run --rm --platform "linux/$a" \
    -v "$REPO:$REPO" -v "$GITDIR:$GITDIR" -v "$OUT:/out" -v "$TARGET_DIR/$a:/target" \
    -v "$CARGO_REGISTRY$a:/usr/local/cargo/registry" \
    -w "$REPO" "$IMAGE:$a" bash -euo pipefail -c "$*"
}

build_native() {
  local a=$1 dir=$2 rustflags=$3
  run "$a" "cd native && RUSTFLAGS='$rustflags' CARGO_TARGET_DIR=/target CARGO_PROFILE_RELEASE_DEBUG=false \
    cargo build --release -j $CARGO_JOBS -p datafusion-comet && cp /target/release/libcomet.so /out/linux/$dir/libcomet.so"
}

build_native arm64 aarch64 ""
build_native amd64 amd64 "-Ctarget-cpu=x86-64-v3"

check_glibc() {
  local a=$1 dir=$2 machine=$3
  local lib="/out/linux/$dir/libcomet.so"
  run "$a" "readelf -h $lib | grep -q '$machine' \
    && max=\$(objdump -T $lib | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1) \
    && echo \"$dir: \$max\" \
    && [ \"\$(printf '%s\n' \"\${max#GLIBC_}\" '$MAX_GLIBC' | sort -V | tail -1)\" = '$MAX_GLIBC' ]"
}

check_glibc arm64 aarch64 AArch64
check_glibc amd64 amd64 X86-64

O=()
[ "${MVN_OFFLINE:-0}" = 1 ] && O=(-o)
rm -f "$REPO/spark/target/$JARNAME" "$REPO/contrib/delta-spark/target/$DELTANAME"
docker run --rm --platform linux/arm64 \
  -v "$REPO:$REPO" -v "$GITDIR:$GITDIR" -v "$OUT:/out" -v "$M2_DIR:/root/.m2" \
  -w "$REPO" "$IMAGE:arm64" \
  ./mvnw "${O[@]}" -q -B package -Prelease -Pspark-3.5,scala-2.12,delta -pl common,spark,contrib/delta-spark \
    -DskipTests -Dspotless.check.skip=true -Dmaven.gitcommitid.skip=true -Djni.dir=/out/linux/aarch64

cp "$REPO/spark/target/$JARNAME" "$REPO/contrib/delta-spark/target/$DELTANAME" "$OUT/"
STAGE="$(mktemp -d)"
mkdir -p "$STAGE/org/apache/comet/linux/amd64"
cp "$OUT/linux/amd64/libcomet.so" "$STAGE/org/apache/comet/linux/amd64/libcomet.so"
(cd "$STAGE" && zip -q -r "$OUT/$JARNAME" org/apache/comet/linux/amd64/libcomet.so)
rm -rf "$STAGE"

JAR="$OUT/$JARNAME"
for dir in aarch64 amd64; do
  cmp "$OUT/linux/$dir/libcomet.so" <(unzip -p "$JAR" "org/apache/comet/linux/$dir/libcomet.so")
done
test "$(unzip -l "$JAR" | grep -c 'libcomet\.so$')" = 2
! unzip -l "$JAR" | grep -q 'libcomet\.dylib$'
unzip -l "$OUT/$DELTANAME" | grep -q DeltaScanContrib

echo "$COMMIT" > "$OUT/commit.txt"
(cd "$OUT" && shasum -a 256 "$JARNAME" "$DELTANAME" linux/*/libcomet.so | tee sha256.txt)
