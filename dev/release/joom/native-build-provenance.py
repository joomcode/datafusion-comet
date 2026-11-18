#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Record and check the immutable inputs between native and packaging RC jobs."""

import argparse
import hashlib
import json
from pathlib import Path
import re

FILES = ("libcomet.so", "cargo-metadata.json", "toolchain.json", "elf-imports.txt")
FLAGS = {"aarch64": "", "amd64": "-Ctarget-cpu=x86-64-v3"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate(out, arch, commit, lock_sha256):
    directory = out / "linux" / arch
    proof = json.loads((directory / "build-provenance.json").read_text())
    require(proof["schema_version"] == 1 and proof["architecture"] == arch,
            f"wrong native stage identity: {arch}")
    require(proof["source_commit"] == commit, f"native commit mismatch: {arch}")
    require(proof["cargo_lock_sha256"] == lock_sha256, f"native lockfile mismatch: {arch}")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", proof["builder_image"]) is not None,
            f"invalid builder image identity: {arch}")
    require(proof["native_profile"] == "release" and proof["cargo_profile_release_debug"] is False,
            f"wrong native Cargo profile: {arch}")
    require(type(proof["cargo_jobs"]) is int and proof["cargo_jobs"] > 0,
            f"invalid native build parallelism: {arch}")
    require(proof["rustflags"] == FLAGS[arch], f"wrong native flags: {arch}")
    require(set(proof["files"]) == set(FILES), f"incomplete native stage: {arch}")
    for name in FILES:
        require(not (directory / name).is_symlink(), f"symlinked native input: {arch}/{name}")
        require(sha256(directory / name) == proof["files"][name],
                f"native input checksum mismatch: {arch}/{name}")
    tool = json.loads((directory / "toolchain.json").read_text())
    require(tool["rustflags"] == FLAGS[arch], f"toolchain/native flags mismatch: {arch}")
    # ELF/toolchain/graph/release-JAR checks additionally run in verify-release.py.
    return proof


def record(out, arch, commit, lock_sha256, image, cargo_jobs):
    directory = out / "linux" / arch
    proof = {"schema_version": 1, "architecture": arch, "source_commit": commit,
             "cargo_lock_sha256": lock_sha256, "builder_image": image,
             "native_profile": "release", "cargo_profile_release_debug": False,
             "cargo_jobs": cargo_jobs, "rustflags": FLAGS[arch],
             "files": {name: sha256(directory / name) for name in FILES}}
    (directory / "build-provenance.json").write_text(json.dumps(proof, indent=2) + "\n")
    return validate(out, arch, commit, lock_sha256)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["record", "validate"])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--lock-sha256", required=True)
    parser.add_argument("--arch", choices=list(FLAGS))
    parser.add_argument("--image")
    parser.add_argument("--cargo-jobs", type=int)
    args = parser.parse_args()
    if re.fullmatch(r"[0-9a-f]{40}", args.commit) is None:
        parser.error("expected a full Git commit")
    if re.fullmatch(r"[0-9a-f]{64}", args.lock_sha256) is None:
        parser.error("expected a SHA256 lockfile digest")
    try:
        if args.operation == "record":
            if args.arch is None or args.image is None or args.cargo_jobs is None:
                parser.error("record requires --arch, --image and --cargo-jobs")
            record(args.out, args.arch, args.commit, args.lock_sha256, args.image, args.cargo_jobs)
            print(f"Recorded native release input: {args.arch} ({args.commit})")
        else:
            proofs = {arch: validate(args.out, arch, args.commit, args.lock_sha256) for arch in FLAGS}
            print(json.dumps(proofs, indent=2))
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.exit(1, f"Native input validation failed: {error}\n")


if __name__ == "__main__":
    main()
