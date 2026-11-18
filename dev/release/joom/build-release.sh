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

die() { echo "$*" >&2; exit 1; }
ensure_clean() {
  local source_status
  source_status="$(git -C "$REPO" status --porcelain --untracked-files=all)" || die "cannot inspect source tree"
  [ -z "$source_status" ] || die "source tree is not clean"
}
usage() {
  cat >&2 <<'USAGE'
Usage: dev/release/joom/build-release.sh <out-dir>
       dev/release/joom/build-release.sh --native <arm64|amd64> <out-dir>
       dev/release/joom/build-release.sh --package <arm64|amd64> <out-dir>

The native stage produces one architecture's library and checked provenance.
The package stage consumes both native stages without rebuilding either library.
Its architecture selects the Maven builder, not the contents of the final JAR.

Build clean HEAD for Spark 4.2.1 / Scala 2.13.18 / Delta 4.4.1.
Ubuntu 20.04 libraries for Linux arm64 and amd64 (x86-64-v3), GLIBC <= 2.31.
The output contains two JARs, libraries, checksums and source/build provenance.
This command builds locally; it does not publish or create a Git tag.

Environment:
  PREPARE_SMOKE=1 package matching portable test support after release validation
  DOCKER_CONTEXT   optional Docker context
  M2_DIR           Maven home mounted into builder (default ~/.m2)
  MVN_OFFLINE=1    Maven offline mode
  CARGO_JOBS       Cargo parallel jobs (default 4)
  TARGET_DIR       architecture-specific Cargo targets (default <out-dir>/cargo-target)
  WORK_DIR         staging directory (default <out-dir>/.work)
  CARGO_REGISTRY   per-architecture registry volume prefix (default comet-joom-1.2-cargo-)
  CARGO_GIT        git source cache volume (default comet-joom-1.2-cargo-git)
USAGE
  exit 2
}

MODE=all
PACKAGE_ARCH=arm64
BUILD_ARCH=
if [ $# -eq 1 ]; then
  OUTPUT_ARG=$1
elif [ $# -eq 3 ]; then
  case "$1" in
    --native) MODE=native; BUILD_ARCH=$2 ;;
    --package) MODE=package; PACKAGE_ARCH=$2 ;;
    *) usage ;;
  esac
  case "$2" in arm64|amd64) ;; *) die "unsupported architecture: $2" ;; esac
  OUTPUT_ARG=$3
else
  usage
fi
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO"
ensure_clean
COMMIT="$(git -C "$REPO" rev-parse HEAD)"
LOCK_SHA="$(shasum -a 256 "$REPO/native/Cargo.lock" | awk '{print $1}')"
OUT="$(mkdir -p "$OUTPUT_ARG" && cd "$OUTPUT_ARG" && pwd)"
[ ! -e "$OUT/sha256.txt" ] || die "release outputs already exist in $OUT; choose another directory"
M2_DIR="${M2_DIR:-$HOME/.m2}"
CARGO_JOBS="${CARGO_JOBS:-4}"
[[ "$CARGO_JOBS" =~ ^[1-9][0-9]*$ ]] || die "invalid CARGO_JOBS"
TARGET_DIR="${TARGET_DIR:-$OUT/cargo-target}"
WORK_DIR="${WORK_DIR:-$OUT/.work}"
CARGO_REGISTRY="${CARGO_REGISTRY:-comet-joom-1.2-cargo-}"
CARGO_GIT="${CARGO_GIT:-comet-joom-1.2-cargo-git}"
IMAGE=comet-joom-builder-focal
JARNAME=comet-spark-spark4.2_2.13-1.2.0.jar
DELTANAME=comet-contrib-delta-spark4.2_2.13-1.2.0.jar
GITDIR="$(cd "$REPO" && cd "$(git rev-parse --git-common-dir)" && pwd)"
DOCKER=(docker)
[ -z "${DOCKER_CONTEXT:-}" ] || DOCKER+=(--context "$DOCKER_CONTEXT")
mkdir -p "$OUT/linux/aarch64" "$OUT/linux/amd64" "$TARGET_DIR/arm64" "$TARGET_DIR/amd64" "$WORK_DIR" "$M2_DIR"
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
# Focal's security-patched Git 2.25 ignores safe.directory supplied with -c.
# Trust only this bind-mounted checkout in container system scope. The file
# lives in this build's staging directory; the host's Git config is untouched.
git config --file "$WORK_DIR/gitconfig" --replace-all safe.directory "$REPO"

ARM_IMAGE=
AMD_IMAGE=
build_image() {
  local arch=$1 image_id
  "${DOCKER[@]}" build --platform "linux/$arch" -t "$IMAGE:1.2-$arch" "$REPO/dev/release/joom"
  image_id="$("${DOCKER[@]}" image inspect --format '{{.Id}}' "$IMAGE:1.2-$arch")"
  case "$arch" in arm64) ARM_IMAGE=$image_id ;; amd64) AMD_IMAGE=$image_id ;; esac
}

run() {
  local arch=$1 builder
  shift
  case "$arch" in arm64) builder=$ARM_IMAGE ;; amd64) builder=$AMD_IMAGE ;; *) die "unsupported architecture" ;; esac
  [ -n "$builder" ] || die "builder image was not selected for $arch"
  "${DOCKER[@]}" run --rm -i --platform "linux/$arch" \
    -v "$REPO:$REPO" -v "$GITDIR:$GITDIR:ro" -v "$OUT:/out" \
    -v "$TARGET_DIR/$arch:/target" -v "$M2_DIR:/root/.m2" \
    -v "$WORK_DIR/gitconfig:/etc/gitconfig:ro" \
    -v "$CARGO_REGISTRY$arch:/usr/local/cargo/registry" \
    -v "$CARGO_GIT:/usr/local/cargo/git" \
    -w "$REPO" "$builder" "$@"
}

# Avoid empty arrays under nounset in macOS Bash 3.2.
maven() {
  if [ "${MVN_OFFLINE:-0}" = 1 ]; then
    run "$PACKAGE_ARCH" ./mvnw -o "$@"
  else
    run "$PACKAGE_ARCH" ./mvnw "$@"
  fi
}

build_native() {
  local arch=$1 libdir=$2 flags=$3
  run "$arch" env CARGO_TARGET_DIR=/target CARGO_PROFILE_RELEASE_DEBUG=false \
    RUSTFLAGS="$flags" bash -euo pipefail -c '
      cd native
      cargo build --locked --release -j "$1" -p datafusion-comet
      cp /target/release/libcomet.so "/out/linux/$2/libcomet.so"
    ' _ "$CARGO_JOBS" "$libdir"
  run "$arch" env RUSTFLAGS="$flags" python3 - "$libdir" <<'PYCODE'
import json, os, pathlib, subprocess, sys
out = pathlib.Path('/out/linux') / sys.argv[1]
def command(*args):
    # Cargo writes warnings to stderr; never mix them into its JSON stdout.
    return subprocess.check_output(args, text=True).strip()
metadata = json.loads(command('cargo', 'metadata', '--locked', '--format-version', '1',
                              '--manifest-path', 'native/Cargo.toml'))
(out/'cargo-metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
tools = {'rustc':command('rustc','-vV'), 'cargo':command('cargo','--version'),
         'javac':command('javac','-version'), 'gcc':command('gcc-10','-dumpfullversion'),
         'protoc':command('protoc','--version'), 'libc':command('ldd','--version'),
         'rustflags':os.environ['RUSTFLAGS']}
(out/'toolchain.json').write_text(json.dumps(tools, indent=2)+'\n')
(out/'elf-imports.txt').write_text(command('objdump','-T',str(out/'libcomet.so'))+'\n')
PYCODE
}
ensure_unchanged() {
  [ "$(git -C "$REPO" rev-parse HEAD)" = "$COMMIT" ] || die "HEAD changed during build"
  ensure_clean
  [ "$(shasum -a 256 "$REPO/native/Cargo.lock" | awk '{print $1}')" = "$LOCK_SHA" ] || die "lockfile changed during build"
}

native_stage() {
  local arch=$1 libdir flags image_id
  case "$arch" in
    arm64) libdir=aarch64; flags=; image_id=$ARM_IMAGE ;;
    amd64) libdir=amd64; flags=-Ctarget-cpu=x86-64-v3; image_id=$AMD_IMAGE ;;
    *) die "unsupported architecture: $arch" ;;
  esac
  build_native "$arch" "$libdir" "$flags"
  ensure_unchanged
  python3 "$REPO/dev/release/joom/native-build-provenance.py" record \
    --out "$OUT" --arch "$libdir" --commit "$COMMIT" --lock-sha256 "$LOCK_SHA" \
    --image "$image_id" --cargo-jobs "$CARGO_JOBS"
}

case "$MODE" in
  all)
    build_image arm64
    build_image amd64
    native_stage arm64
    native_stage amd64
    ;;
  native)
    build_image "$BUILD_ARCH"
    native_stage "$BUILD_ARCH"
    printf 'Built native %s (%s)\n' "$BUILD_ARCH" "$COMMIT"
    exit 0
    ;;
  package)
    :
    ;;
esac

# Reject stale, cross-commit, partial or damaged stage inputs before packaging.
python3 "$REPO/dev/release/joom/native-build-provenance.py" validate \
  --out "$OUT" --commit "$COMMIT" --lock-sha256 "$LOCK_SHA" \
  > "$OUT/native-inputs.json"
if [ "$MODE" = package ]; then
  build_image "$PACKAGE_ARCH"
fi
case "$PACKAGE_ARCH" in arm64) PACKAGE_LIBDIR=aarch64 ;; amd64) PACKAGE_LIBDIR=amd64 ;; esac

# Only the root parent-model transfer is retried, before compilation or tests.
# Respect the existing explicit offline mode for a fully populated local cache.
if [ "${MVN_OFFLINE:-0}" != 1 ]; then
  run "$PACKAGE_ARCH" ./dev/ci/prepare-maven-model.sh -Pspark-4.2,delta
fi
maven -B -ntp -Pspark-4.2,delta help:effective-pom -Doutput=/out/effective-pom.xml
python3 - "$OUT/effective-pom.xml" <<'PYCODE'
import pathlib, shutil, sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
project = root if root.tag.endswith('}project') else root.find('m:project', ns)
props = project.find('m:properties', ns)
for key, value in {'spark.version':'4.2.1', 'scala.version':'2.13.18',
                   'delta.spark.version':'4.4.1', 'delta.artifact':'delta-spark_4.2'}.items():
    if props.find('m:'+key, ns).text != value:
        raise SystemExit('effective POM mismatch: '+key)
repo = pathlib.Path.cwd().resolve()
projects = [root] if root.tag.endswith('}project') else root.findall('m:project', ns)
outputs = []
for project in projects:
    output = pathlib.Path(project.find('m:build/m:directory', ns).text).resolve()
    if output == repo/'target':
        continue  # Preserve the Spark fixture and caller outputs under root target.
    output.relative_to(repo)  # Fail before deleting an output outside the checkout.
    if output.name != 'target':
        raise SystemExit('refusing to remove non-target output: '+str(output))
    outputs.append(output)
if not outputs:
    raise SystemExit('no Maven module outputs found')
for output in outputs:
    if output.exists():
        shutil.rmtree(output)
PYCODE

# Compile matching test JARs required by Delta, but do not run tests here.
# Regenerate embedded Git metadata rather than reusing a host build's revision.
if [ "${PREPARE_SMOKE:-0}" = 1 ]; then
  # Keep classpath resolution in this reactor session. A fresh runner has not
  # installed our internal common/test artifacts in Maven Local, and a second
  # dependency-only invocation would incorrectly try to fetch them from Central.
  maven -B -ntp package dependency:build-classpath -Prelease,spark-4.2,delta \
    -DskipTests -Djni.dir="/out/linux/$PACKAGE_LIBDIR" \
    -DincludeScope=test -Dmdep.outputFile=target/rc-smoke-classpath.txt
else
  maven -B -ntp package -Prelease,spark-4.2,delta \
    -DskipTests -Djni.dir="/out/linux/$PACKAGE_LIBDIR"
fi
cp "$REPO/spark/target/$JARNAME" "$REPO/contrib/delta-spark/target/$DELTANAME" "$OUT/"
STAGE="$(mktemp -d "$WORK_DIR/stage.XXXXXXXX")"
trap 'rm -rf "$STAGE"' EXIT
# Maven embeds its host-architecture JNI input. Add the other architecture once;
# the validator rejects duplicate entries, stale host libraries and mismatched bytes.
case "$PACKAGE_LIBDIR" in aarch64) OTHER_LIBDIR=amd64 ;; amd64) OTHER_LIBDIR=aarch64 ;; esac
mkdir -p "$STAGE/org/apache/comet/linux/$OTHER_LIBDIR"
cp "$OUT/linux/$OTHER_LIBDIR/libcomet.so" "$STAGE/org/apache/comet/linux/$OTHER_LIBDIR/libcomet.so"
(cd "$STAGE" && zip -q "$OUT/$JARNAME" "org/apache/comet/linux/$OTHER_LIBDIR/libcomet.so")

ensure_unchanged
printf '%s\n' "$COMMIT" > "$OUT/commit.txt"
python3 - "$OUT" "$COMMIT" "$LOCK_SHA" "$CARGO_JOBS" "$PACKAGE_ARCH" <<'PYCODE'
import hashlib, json, pathlib, sys
out = pathlib.Path(sys.argv[1])
tools = {arch:json.loads((out/'linux'/arch/'toolchain.json').read_text())
         for arch in ['aarch64','amd64']}
metadata = json.loads((out/'linux/aarch64/cargo-metadata.json').read_text())
versions = {p['name']:p['version'] for p in metadata['packages']}
native_builds = json.loads((out/'native-inputs.json').read_text())
manifest = {'commit':sys.argv[2], 'cargo_lock_sha256':sys.argv[3],
            'versions':{'comet':versions['datafusion-comet'],'spark':'4.2.1',
                        'scala':'2.13.18','delta':'4.4.1','datafusion':versions['datafusion'],
                        'arrow_parquet':versions['arrow'],
                        'rust':next(line.split(': ',1)[1] for line in tools['aarch64']['rustc'].splitlines() if line.startswith('release: ')),
                        'java':tools['aarch64']['javac'].split()[1].split('.')[0]},
            'builder_images':{'arm64':native_builds['aarch64']['builder_image'],
                              'amd64':native_builds['amd64']['builder_image']},
            'native_builds':native_builds, 'packaging_architecture':sys.argv[5],
            'native_profile':'release', 'cargo_jobs':int(sys.argv[4]),
            'cargo_profile_release_debug':False,
            'native_flags':{arch:tool['rustflags'] for arch,tool in tools.items()},
            'max_glibc':'2.31', 'qualification':'See docs/joom-migration; G01 RC is an incomplete Joom migration'}
(out/'build-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
files = ['comet-spark-spark4.2_2.13-1.2.0.jar','comet-contrib-delta-spark4.2_2.13-1.2.0.jar',
         'linux/aarch64/libcomet.so','linux/amd64/libcomet.so','commit.txt','build-manifest.json','effective-pom.xml']
files += ['linux/'+arch+'/'+name for arch in ['aarch64','amd64']
          for name in ['cargo-metadata.json','toolchain.json','elf-imports.txt']]
(out/'sha256.txt').write_text(''.join(hashlib.sha256((out/f).read_bytes()).hexdigest()+'  '+f+'\n' for f in files))
PYCODE
python3 "$REPO/dev/release/joom/verify-release.py" "$OUT" --commit "$COMMIT" --lock-sha256 "$LOCK_SHA"
if [ "${PREPARE_SMOKE:-0}" = 1 ]; then
  run "$PACKAGE_ARCH" python3 dev/release/joom/verify-packaged-runtime.py prepare \
    --repo "$REPO" --release /out --commit "$COMMIT" --lock-sha256 "$LOCK_SHA"
  ensure_unchanged
fi
printf 'Built %s (%s)\n' "$OUT" "$COMMIT"
