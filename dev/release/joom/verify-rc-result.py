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

"""Export release bytes only when both native architectures tested the same package."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
CORE = "comet-spark-spark4.2_2.13-1.2.0.jar"
DELTA = "comet-contrib-delta-spark4.2_2.13-1.2.0.jar"
COUNTS = {"delta": 4, "dpp": 1, "reload": 1, "ansi": 6,
          "g08-udf": 1, "g08-delta-serialization": 2, "g09-cost": 4,
          "g09-wide-sort": 1, "g09-wide-shuffle": 1, "g09-range": 1,
          "g07-kryo": 1, "g10-udf-shuffle": 2, "g10-expression-guards": 2}
CORE_ORIGIN_CLASSES = (
    "org.apache.comet.NativeBase",
    "org.apache.comet.udf.codegen.CometUdfFunctionPool$",
    "org.apache.comet.udf.codegen.CometUdfFunctionPool$PlanPayload",
    "org.apache.comet.udf.codegen.CometSharedFunctionRef",
    "org.apache.comet.udf.codegen.CometSharedFunctionRef$",
    "org.apache.comet.udf.codegen.CometScalaUDFCodegen",
    "org.apache.comet.serde.QueryContextInterner$",
    "org.apache.comet.serde.QueryContextInterner$Pool",
    "org.apache.comet.rules.BoundaryFormats$",
    "org.apache.comet.rules.ChooseBoundaryFormats",
    "org.apache.comet.rules.CostBasedEngineChoice",
    "org.apache.comet.rules.EngineCostModel",
    "org.apache.comet.rules.WideRowSortFallback",
    "org.apache.comet.rules.WideRowShuffleFallback$",
    "org.apache.spark.CometPlugin",
    "org.apache.comet.CometKryoRegistrator",
    "org.apache.spark.sql.comet.CometBroadcastExchangeExec",
    "org.apache.spark.sql.comet.execution.shuffle.NativeShuffleSpec",
    "org.apache.spark.sql.comet.execution.shuffle.NativeShuffleSpec$",
    "org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec",
    "org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec$",
    "org.apache.spark.sql.comet.execution.shuffle.CometNativeShuffleWriter",
    "org.apache.comet.expressions.CometCast",
    "org.apache.comet.expressions.CometCast$",
)
DELTA_ORIGIN_CLASSES = (
    "org.apache.spark.sql.comet.CometDeltaNativeScanExec",
    "org.apache.spark.sql.comet.DeltaPlanDataInjector",
)
DELTA_GROUPS = {"delta", "g08-delta-serialization"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def module(name):
    path = Path(__file__).resolve().with_name(name + ".py")
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def unique_receipt_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON field in smoke receipt: " + key)
        value[key] = item
    return value


def validate_class_origins(proof, group, jar_hashes):
    expected = {name: CORE for name in CORE_ORIGIN_CLASSES}
    if group in DELTA_GROUPS:
        expected.update({name: DELTA for name in DELTA_ORIGIN_CLASSES})
    origins = proof.get("class_origins")
    require(isinstance(origins, dict) and set(origins) == set(expected),
            "missing or extra final-JAR class origins: " + group)
    for name, jar in expected.items():
        origin = origins[name]
        require(isinstance(origin, dict)
                and set(origin) == {"jar_name", "jar_sha256", "resource_count"}
                and origin["jar_name"] == jar
                and origin["jar_sha256"] == jar_hashes[jar]
                and type(origin["resource_count"]) is int
                and origin["resource_count"] == 1,
                "wrong or nonunique final-JAR class origin: " + group + "/" + name)


def validate_case_proof(case, arch, jar_hashes, native_hash):
    """Check one receipt group without filesystem access, including final-JAR origins."""
    group = case["group"]
    require(group in COUNTS, "unexpected smoke group: " + group)
    require(case["passed"] == COUNTS[group]
            and all(case[key] == 0 for key in ["failed", "canceled", "ignored", "pending"])
            and case["bundled_native_and_class_origins"] == "VERIFIED",
            "smoke group failed or skipped cases: " + arch + "/" + group)
    proof = case.get("loaded_native_proof")
    require(isinstance(proof, dict), "missing loaded-native proof: " + arch)
    require(proof["architecture"] == arch
            and proof["core_jar_sha256"] == jar_hashes[CORE]
            and proof["loaded_native_sha256"] == native_hash
            and proof["java_version"].split(".")[0] == "17"
            and proof["spark_version"] == "4.2.1",
            "missing or inconsistent loaded-native proof: " + arch)
    validate_class_origins(proof, group, jar_hashes)


def validate_receipts(out, reports, commit, lock):
    jar_hashes = {name: digest(out / name) for name in [CORE, DELTA]}
    checksum_hash = digest(out / "sha256.txt")
    support_hash = digest(out / "test-support/manifest.json")
    files = list(reports.rglob("results.json"))
    require(len(files) == 2, "expected exactly two packaged smoke receipts")
    receipts = {}
    for path in files:
        result = json.loads(path.read_text(), object_pairs_hook=unique_receipt_object)
        arch = result["architecture"]
        require(arch in ["amd64", "aarch64"] and arch not in receipts,
                "duplicate or unexpected smoke architecture")
        require(result["commit"] == commit and result["cargo_lock_sha256"] == lock,
                "smoke source/lockfile mismatch: " + arch)
        require(result["release_jar_sha256"] == jar_hashes,
                "smoke tested different final JARs: " + arch)
        require(result["checksum_manifest_sha256"] == checksum_hash
                and result["test_support_manifest_sha256"] == support_hash,
                "smoke tested a different package or test support: " + arch)
        native_hash = digest(out / "linux" / arch / "libcomet.so")
        require(result["native_sha256"] == native_hash, "smoke native bytes mismatch: " + arch)
        cases = result["cases"]
        require(len(cases) == len(COUNTS)
                and {case["group"] for case in cases} == set(COUNTS),
                "missing, duplicate or extra smoke groups: " + arch)
        require(result["total_passed"] == sum(COUNTS.values()), "incomplete smoke: " + arch)
        for case in cases:
            validate_case_proof(case, arch, jar_hashes, native_hash)
        receipts[arch] = {"result": result, "receipt_sha256": digest(path),
                          "directory": path.parent}
    require(set(receipts) == {"amd64", "aarch64"}, "both architectures must pass")
    return receipts


def export(out, reports, commit, lock, destination):
    module("verify-release").validate(out, commit, lock)
    module("verify-packaged-runtime").validate_support(out, commit, lock)
    native = module("native-build-provenance")
    for arch in ["amd64", "aarch64"]:
        native.validate(out, arch, commit, lock)
    receipts = validate_receipts(out, reports, commit, lock)
    require(not destination.exists(), "qualified output already exists; preserve it")
    destination.mkdir(parents=True)
    # Copy the original checksummed bytes. Never repackage the JAR after smoke.
    names = [line.split("  ", 1)[1] for line in (out / "sha256.txt").read_text().splitlines()]
    for name in names + ["sha256.txt"]:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(out / name, target)
    for arch, receipt in receipts.items():
        shutil.copytree(receipt["directory"], destination / "smoke" / arch)
        shutil.copy2(out / "linux" / arch / "build-provenance.json",
                     destination / "linux" / arch / "build-provenance.json")
    proof = {"source_commit": commit, "cargo_lock_sha256": lock,
             "qualification": "RELEASE_BUILD_AND_PACKAGED_SMOKE_PASSED",
             "scope": "Targeted final-JAR runtime smoke for Delta, DPP, native reload, ANSI, "
                      "G07 default Kryo, G08 UDF/serialization and G09 engine/boundary paths; "
                      "full Comet CI and production GCP qualification are separate",
             "packaged_smoke_groups": COUNTS,
             "release_jar_sha256": {name: digest(destination / name) for name in [CORE, DELTA]},
             "checksum_manifest_sha256": digest(destination / "sha256.txt"),
             "test_support_manifest_sha256": digest(out / "test-support/manifest.json"),
             "architectures": {arch: {"passed": receipt["result"]["total_passed"],
                                      "receipt_sha256": receipt["receipt_sha256"]}
                               for arch, receipt in receipts.items()},
             "github_run_id": os.environ.get("GITHUB_RUN_ID"),
             "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT")}
    (destination / "release-qualification.json").write_text(json.dumps(proof, indent=2) + "\n")
    module("verify-release").validate(destination, commit, lock)
    print("Both architectures passed the same final release bytes: " + commit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--smoke-reports", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--lock-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        export(args.release, args.smoke_reports, args.commit, args.lock_sha256, args.output)
    except (ValueError, OSError, KeyError, TypeError, AttributeError) as error:
        parser.exit(1, "RC qualification failed: " + str(error) + "\n")


if __name__ == "__main__":
    main()
