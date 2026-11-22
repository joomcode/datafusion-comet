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
                     "spark_version": "4.2.1", "required_sql_conf": dict(rc.REQUIRED_SQL_CONF)}
            cases = []
            for group, count in rc.COUNTS.items():
                origins = {name: {"jar_name": rc.CORE, "jar_sha256": jars[rc.CORE],
                                  "resource_count": 1} for name in rc.CORE_ORIGIN_CLASSES}
                if group in rc.DELTA_GROUPS:
                    origins.update({name: {"jar_name": rc.DELTA,
                                           "jar_sha256": jars[rc.DELTA], "resource_count": 1}
                                    for name in rc.DELTA_ORIGIN_CLASSES})
                loaded = dict(copy.deepcopy(proof), class_origins=origins)
                cases.append({"group": group, "passed": count, "failed": 0,
                              "canceled": 0, "ignored": 0, "pending": 0,
                              "bundled_native_and_class_origins": "VERIFIED",
                              "observed_session_defaults": [dict(rc.REQUIRED_SQL_CONF)],
                              "loaded_native_proof": loaded})
            result = {"architecture": arch, "commit": COMMIT, "cargo_lock_sha256": LOCK,
                      "required_sql_conf": dict(rc.REQUIRED_SQL_CONF),
                      "release_jar_sha256": jars, "native_sha256": rc.digest(native),
                      "checksum_manifest_sha256": rc.digest(self.out / "sha256.txt"),
                      "test_support_manifest_sha256": rc.digest(self.out / "test-support/manifest.json"),
                      "total_passed": sum(rc.COUNTS.values()), "cases": cases}
            path = self.reports / arch / "results.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(result))
            self.results[arch] = (path, result)

    def validate(self):
        return rc.validate_receipts(self.out, self.reports, COMMIT, LOCK)

    def rewrite(self, arch):
        path, result = self.results[arch]
        path.write_text(json.dumps(result))

    def case(self, group, arch="amd64"):
        return next(case for case in self.results[arch][1]["cases"] if case["group"] == group)

    def test_both_architectures_accept_identical_final_bytes(self):
        self.assertEqual(set(self.validate()), {"amd64", "aarch64"})

    def test_case_proof_does_not_need_the_test_support_manifest(self):
        result = self.results["amd64"][1]
        (self.out / "test-support/manifest.json").unlink()
        rc.validate_case_proof(self.case("g09-cost"), "amd64",
                               result["release_jar_sha256"], result["native_sha256"])

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

    def test_missing_or_incompatible_session_defaults_cannot_qualify_release(self):
        case = self.case("g09-cost")
        original = copy.deepcopy(case["observed_session_defaults"])
        for defaults in [None, [],
                         [dict(rc.REQUIRED_SQL_CONF, **{"spark.sql.columnVector.offheap.enabled": "true"})],
                         [dict(rc.REQUIRED_SQL_CONF, **{"spark.sql.ansi.enabled": "false"})]]:
            case["observed_session_defaults"] = defaults
            self.rewrite("amd64")
            with self.subTest(defaults=defaults), self.assertRaisesRegex(ValueError, "session defaults"):
                self.validate()
        case["observed_session_defaults"] = original

    def test_declared_contract_cannot_replace_observed_session_defaults(self):
        case = self.case("g09-cost")
        case["loaded_native_proof"].pop("required_sql_conf")
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "session defaults"):
            self.validate()

    def test_receipt_requires_the_production_sql_contract(self):
        self.results["amd64"][1].pop("required_sql_conf")
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "required Spark SQL configuration"):
            self.validate()

    def test_exporter_contract_matches_runtime_runner(self):
        runtime = rc.module("verify-packaged-runtime")
        self.assertEqual(rc.COUNTS, {label: count for label, _, _, count in runtime.CASES})
        self.assertEqual(len(runtime.CASES), 13)
        self.assertEqual(sum(rc.COUNTS.values()), 27)
        self.assertEqual(set(rc.CORE_ORIGIN_CLASSES), set(runtime.CORE_ORIGIN_CLASSES))
        self.assertEqual(set(rc.DELTA_ORIGIN_CLASSES), set(runtime.DELTA_ORIGIN_CLASSES))
        self.assertEqual(rc.DELTA_GROUPS, set(runtime.DELTA_GROUPS))
        self.assertEqual(rc.REQUIRED_SQL_CONF, runtime.REQUIRED_SQL_CONF)
        self.assertEqual(len(runtime.CORE_ORIGIN_CLASSES), 24)
        self.assertEqual(len(runtime.DELTA_ORIGIN_CLASSES), 2)

    def test_legacy_twelve_case_smoke_cannot_qualify_expanded_release(self):
        result = self.results["amd64"][1]
        result["cases"] = [case for case in result["cases"]
                           if case["group"] in {"delta", "dpp", "reload", "ansi"}]
        result["total_passed"] = 12
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "missing, duplicate or extra smoke groups"):
            self.validate()

    def test_previous_twenty_three_cases_cannot_omit_shuffle_keys(self):
        result = self.results["amd64"][1]
        result["cases"] = [case for case in result["cases"]
                           if case["group"] not in {"g10-udf-shuffle", "g10-expression-guards"}]
        result["total_passed"] = 23
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "missing, duplicate or extra smoke groups"):
            self.validate()

    def test_shuffle_group_requires_both_executed_cases(self):
        case = self.case("g10-udf-shuffle")
        for field, value in [("passed", 1), ("failed", 1), ("canceled", 1),
                             ("ignored", 1), ("pending", 1)]:
            with self.subTest(field=field):
                old = case[field]
                case[field] = value
                self.rewrite("amd64")
                with self.assertRaisesRegex(ValueError, "failed or skipped cases"):
                    self.validate()
                case[field] = old

    def test_shuffle_group_requires_fixed_class_origins(self):
        origins = self.case("g10-udf-shuffle")["loaded_native_proof"]["class_origins"]
        fixed = (
            "org.apache.spark.sql.comet.execution.shuffle.NativeShuffleSpec",
            "org.apache.spark.sql.comet.execution.shuffle.NativeShuffleSpec$",
            "org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec",
            "org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec$",
            "org.apache.spark.sql.comet.execution.shuffle.CometNativeShuffleWriter",
        )
        self.assertTrue(set(fixed).issubset(origins))
        for name in fixed:
            old = origins.pop(name)
            self.rewrite("amd64")
            with self.subTest(missing=name), self.assertRaisesRegex(
                    ValueError, "missing or extra final-JAR class origins"):
                self.validate()
            origins[name] = old
        origins[fixed[0]]["jar_name"] = "core-tests.jar"
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "wrong or nonunique final-JAR class origin"):
            self.validate()

    def test_previous_twenty_five_cases_cannot_omit_expression_guards(self):
        result = self.results["amd64"][1]
        result["cases"] = [case for case in result["cases"]
                           if case["group"] != "g10-expression-guards"]
        result["total_passed"] = 25
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "missing, duplicate or extra smoke groups"):
            self.validate()

    def test_expression_guard_group_requires_both_executed_cases(self):
        case = self.case("g10-expression-guards")
        for field, value in [("passed", 1), ("failed", 1), ("canceled", 1),
                             ("ignored", 1), ("pending", 1)]:
            with self.subTest(field=field):
                old = case[field]
                case[field] = value
                self.rewrite("amd64")
                with self.assertRaisesRegex(ValueError, "failed or skipped cases"):
                    self.validate()
                case[field] = old

    def test_expression_guard_group_requires_both_fixed_cast_origins(self):
        origins = self.case("g10-expression-guards")["loaded_native_proof"]["class_origins"]
        fixed = ("org.apache.comet.expressions.CometCast", "org.apache.comet.expressions.CometCast$")
        self.assertTrue(set(fixed).issubset(origins))
        for name in fixed:
            old = origins.pop(name)
            self.rewrite("amd64")
            with self.subTest(missing=name), self.assertRaisesRegex(
                    ValueError, "missing or extra final-JAR class origins"):
                self.validate()
            origins[name] = old
            for field, value in [("jar_name", "core-tests.jar"), ("jar_sha256", "0" * 64),
                                 ("resource_count", 2)]:
                with self.subTest(origin=name, field=field):
                    previous = origins[name][field]
                    origins[name][field] = value
                    self.rewrite("amd64")
                    with self.assertRaisesRegex(ValueError, "wrong or nonunique final-JAR class origin"):
                        self.validate()
                    origins[name][field] = previous

    def test_duplicate_new_group_cannot_hide_a_missing_group(self):
        result = self.results["amd64"][1]
        result["cases"] = [copy.deepcopy(self.case("g09-wide-sort"))
                           if case["group"] == "g09-range" else case
                           for case in result["cases"]]
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "missing, duplicate or extra smoke groups"):
            self.validate()

    def test_new_group_requires_its_full_test_count(self):
        self.case("g09-cost")["passed"] = 3
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "failed or skipped cases"):
            self.validate()

    def test_pending_new_case_cannot_qualify_release(self):
        self.case("g08-delta-serialization")["pending"] = 1
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "failed or skipped cases"):
            self.validate()

    def test_new_group_requires_loaded_native_proof(self):
        del self.case("g08-udf")["loaded_native_proof"]
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "missing loaded-native proof"):
            self.validate()

    def test_verified_label_cannot_replace_class_origin_proof(self):
        del self.case("g09-cost")["loaded_native_proof"]["class_origins"]
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "missing or extra final-JAR class origins"):
            self.validate()

    def test_missing_new_payload_class_cannot_qualify_release(self):
        origins = self.case("g08-udf")["loaded_native_proof"]["class_origins"]
        del origins["org.apache.comet.udf.codegen.CometUdfFunctionPool$PlanPayload"]
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "missing or extra final-JAR class origins"):
            self.validate()

    def test_wrong_or_nonunique_new_core_origin_cannot_qualify_release(self):
        origin = self.case("g09-cost")["loaded_native_proof"]["class_origins"][
            "org.apache.comet.rules.CostBasedEngineChoice"]
        for field, value in [("jar_name", "core-tests.jar"), ("jar_sha256", "0" * 64),
                             ("resource_count", 2)]:
            with self.subTest(field=field):
                old = origin[field]
                origin[field] = value
                self.rewrite("amd64")
                with self.assertRaisesRegex(ValueError, "wrong or nonunique final-JAR class origin"):
                    self.validate()
                origin[field] = old

    def test_new_serialization_group_requires_final_delta_origins(self):
        origins = self.case("g08-delta-serialization")["loaded_native_proof"]["class_origins"]
        origins["org.apache.spark.sql.comet.DeltaPlanDataInjector"]["jar_name"] = rc.CORE
        self.rewrite("amd64")
        with self.assertRaisesRegex(ValueError, "wrong or nonunique final-JAR class origin"):
            self.validate()

    def test_duplicate_class_origin_json_cannot_qualify_release(self):
        path, _ = self.results["amd64"]
        duplicate = json.dumps("org.apache.comet.NativeBase") + ": {}"
        text = path.read_text().replace('"class_origins": {',
                                       '"class_origins": {' + duplicate + ', ', 1)
        path.write_text(text)
        with self.assertRaisesRegex(ValueError, "duplicate JSON field in smoke receipt"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
