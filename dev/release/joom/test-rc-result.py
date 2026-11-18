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

"""Reject stale or incomplete smoke receipts before marking a package qualified."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("rc_result", Path(__file__).with_name("verify-rc-result.py"))
rc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rc)
COMMIT, LOCK = "a" * 40, "b" * 64


class ReceiptBoundary(unittest.TestCase):
    def setUp(self):
        base = os.environ.get("RC_TEST_TMPDIR")
        if not base:
            raise RuntimeError("Set RC_TEST_TMPDIR to an explicit test output directory")
        self.tmp = tempfile.TemporaryDirectory(prefix="rc-receipts-", dir=base)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.out = self.root / "release"
        self.reports = self.root / "reports"
        (self.out / "test-support").mkdir(parents=True)
        for name in [rc.CORE, rc.DELTA, "sha256.txt", "test-support/manifest.json"]:
            (self.out / name).write_text(name)
        self.results = {}
        for arch in ["amd64", "aarch64"]:
            native = self.out / "linux" / arch / "libcomet.so"
            native.parent.mkdir(parents=True)
            native.write_text(arch)
            jars = {name: rc.digest(self.out / name) for name in [rc.CORE, rc.DELTA]}
            proof = {"architecture": arch, "core_jar_sha256": jars[rc.CORE],
                     "loaded_native_sha256": rc.digest(native), "java_version": "17.0.19",
                     "spark_version": "4.2.1"}
            result = {"architecture": arch, "commit": COMMIT, "cargo_lock_sha256": LOCK,
                      "release_jar_sha256": jars, "native_sha256": rc.digest(native),
                      "checksum_manifest_sha256": rc.digest(self.out / "sha256.txt"),
                      "test_support_manifest_sha256": rc.digest(self.out / "test-support/manifest.json"),
                      "total_passed": 12,
                      "cases": [{"group": group, "passed": count, "failed": 0,
                                 "canceled": 0, "ignored": 0,
                                 "bundled_native_and_class_origins": "VERIFIED",
                                 "loaded_native_proof": copy.deepcopy(proof)}
                                for group, count in rc.COUNTS.items()]}
            path = self.reports / arch / "results.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(result))
            self.results[arch] = (path, result)

    def validate(self):
        return rc.validate_receipts(self.out, self.reports, COMMIT, LOCK)

    def rewrite(self, arch):
        path, result = self.results[arch]
        path.write_text(json.dumps(result))

    def test_both_architectures_accept_identical_final_bytes(self):
        self.assertEqual(set(self.validate()), {"amd64", "aarch64"})

    def test_repackaging_invalidates_previously_successful_smoke(self):
        (self.out / rc.CORE).write_text("new final package")
        with self.assertRaisesRegex(ValueError, "different final JARs"):
            self.validate()

    def test_support_change_invalidates_old_receipt(self):
        (self.out / "test-support/manifest.json").write_text("new support")
        with self.assertRaisesRegex(ValueError, "different package or test support"):
            self.validate()

    def test_one_architecture_cannot_stand_for_both(self):
        path, _ = self.results["aarch64"]
        # Both receipts are individually valid for amd64. Only their duplicate
        # identity is invalid, regardless of which filesystem entry is visited first.
        path.write_text(json.dumps(self.results["amd64"][1]))
        with self.assertRaisesRegex(ValueError, "duplicate or unexpected"):
            self.validate()

    def test_cancellation_cannot_qualify_release(self):
        self.results["amd64"][1]["cases"][0]["canceled"] = 1
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "failed or skipped"):
            self.validate()

    def test_other_native_or_runtime_cannot_qualify_release(self):
        for field, value in [("loaded_native_sha256", "0" * 64), ("spark_version", "4.1.4")]:
            result = self.results["amd64"][1]
            old = result["cases"][0]["loaded_native_proof"][field]
            result["cases"][0]["loaded_native_proof"][field] = value
            self.rewrite("amd64")
            with self.assertRaisesRegex(ValueError, "loaded-native proof"):
                self.validate()
            result["cases"][0]["loaded_native_proof"][field] = old


if __name__ == "__main__":
    unittest.main()
