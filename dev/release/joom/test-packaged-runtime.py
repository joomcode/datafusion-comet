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

"""Fault checks: skipped tests, wrong bytes and leaked production classes must fail."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

spec = importlib.util.spec_from_file_location(
    "packaged_runtime", Path(__file__).with_name("verify-packaged-runtime.py"))
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class PackagedRuntimeFaults(unittest.TestCase):
    def test_summary_requires_exact_executed_test_count(self):
        good = ("Suites: completed 1, aborted 0\n"
                "Tests: succeeded 12, failed 0, canceled 0, ignored 0, pending 0\n")
        runtime.assert_test_summary(good, 12)
        for malformed in [
            good.replace("succeeded 12", "succeeded 11"),
            good.replace("failed 0", "failed 1"),
            good.replace("canceled 0", "canceled 1"),
            good.replace("ignored 0", "ignored 1"),
            good.replace("pending 0", "pending 1"),
            good.replace("aborted 0", "aborted 1"),
            good + good,
            "All tests passed.\n",
        ]:
            with self.subTest(summary=malformed), self.assertRaises(ValueError):
                runtime.assert_test_summary(malformed, 12)

    def setUp(self):
        # The caller chooses an explicit task/project output location, never /tmp.
        root = os.environ.get("RC_TEST_TMPDIR")
        if not root:
            raise RuntimeError("Set RC_TEST_TMPDIR to an explicit test output directory")
        self.temporary = tempfile.TemporaryDirectory(prefix="20261118_packaged-runtime-", dir=root)
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def jar(self, name, member="fixture.txt"):
        path = self.root / name
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr(member, "fixture")
        return path

    def test_support_cannot_supply_main_classes_or_native_libraries(self):
        runtime.check_test_jar(self.jar("test.jar"))
        for index, member in enumerate(sorted(runtime.FORBIDDEN_CLASSES)
                                       + ["org/apache/comet/linux/amd64/libcomet.so"]):
            with self.subTest(member=member), self.assertRaises(ValueError):
                runtime.check_test_jar(self.jar("bad-%d.jar" % index, member))

    def test_support_paths_cannot_escape_bundle(self):
        bundle = self.root / "bundle"
        bundle.mkdir()
        (bundle / "ok.jar").write_bytes(b"fixture")
        outside = self.root / "outside.jar"
        outside.write_bytes(b"outside")
        (bundle / "link.jar").symlink_to(outside)
        self.assertEqual(runtime.safe_file(bundle, "ok.jar"), bundle / "ok.jar")
        for path in ["../outside.jar", str(outside), "link.jar", "missing.jar"]:
            with self.subTest(path=path), self.assertRaises(ValueError):
                runtime.safe_file(bundle, path)

    def fixture(self):
        out = self.root / "release"
        out.mkdir()
        (out / runtime.CORE).write_bytes(b"final core bytes")
        (out / runtime.DELTA).write_bytes(b"final delta bytes")
        (out / "effective-pom.xml").write_text("<project />")
        support = out / "test-support"
        support.mkdir()
        (support / "probe").mkdir()
        (support / "probe/RcPackagedSmoke.class").write_bytes(b"fixture class")
        for name in ["core-tests.jar", "delta-tests.jar"]:
            with zipfile.ZipFile(support / name, "w") as archive:
                archive.writestr("test-resource.txt", "fixture")
        manifest = {
            "format": 1, "commit": "a" * 40, "cargo_lock_sha256": "b" * 64,
            "release_jar_sha256": {name: runtime.digest(out / name)
                                   for name in [runtime.CORE, runtime.DELTA]},
            "effective_pom_sha256": runtime.digest(out / "effective-pom.xml"),
            "classpath": ["core-tests.jar", "delta-tests.jar", "probe"],
            "files": {p.relative_to(support).as_posix(): runtime.digest(p)
                      for p in support.rglob("*") if p.is_file()},
        }
        (support / "manifest.json").write_text(json.dumps(manifest))
        return out, manifest

    def test_manifest_binds_support_to_release_and_source(self):
        out, manifest = self.fixture()
        runtime.validate_support(out, "a" * 40, "b" * 64)
        for commit, lock in [("c" * 40, "b" * 64), ("a" * 40, "d" * 64)]:
            with self.assertRaises(ValueError):
                runtime.validate_support(out, commit, lock)
        (out / runtime.CORE).write_bytes(b"different core bytes")
        with self.assertRaises(ValueError):
            runtime.validate_support(out, "a" * 40, "b" * 64)

    def test_test_support_changes_are_not_silent(self):
        out, manifest = self.fixture()
        target = out / "test-support/probe/RcPackagedSmoke.class"
        target.write_bytes(b"stale class")
        with self.assertRaises(ValueError):
            runtime.validate_support(out, "a" * 40, "b" * 64)

    def test_extra_classpath_file_is_rejected(self):
        out, manifest = self.fixture()
        (out / "test-support/extra.jar").write_bytes(b"extra")
        with self.assertRaises(ValueError):
            runtime.validate_support(out, "a" * 40, "b" * 64)

    def test_java_probe_checks_loaded_bytes_and_origin_after_tests(self):
        java = Path(os.environ["JAVA_HOME"]) / "bin"
        source = self.root / "source"
        classes = self.root / "classes"
        probe = self.root / "probe"
        source.mkdir()
        classes.mkdir()
        probe.mkdir()
        files = {
            "NativeBase.java": """package org.apache.comet;
public final class NativeBase {
  private static boolean loaded = true;
  private static java.io.File bundledLibrary =
      new java.io.File(System.getProperty("fake.native.file"));
}
""",
            "Runner.java": """package org.scalatest.tools;
public final class Runner {
  public static boolean run(String[] args) {
    return !Boolean.getBoolean("fake.tests.fail");
  }
}
""",
            "package$.java": """package org.apache.spark;
public final class package$ {
  public static final package$ MODULE$ = new package$();
  public String SPARK_VERSION() {
    return System.getProperty("fake.spark.version", "4.2.1");
  }
}
""",
        }
        for name, text in files.items():
            (source / name).write_text(text)
        subprocess.run([str(java / "javac"), "--release", "17", "-d", str(classes)]
                       + [str(source / name) for name in files], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        core = self.root / "core.jar"
        with zipfile.ZipFile(core, "w") as jar:
            path = classes / "org/apache/comet/NativeBase.class"
            jar.write(path, "org/apache/comet/NativeBase.class")
        runner = self.root / "runner.jar"
        with zipfile.ZipFile(runner, "w") as jar:
            for member in ["org/scalatest/tools/Runner.class", "org/apache/spark/package$.class"]:
                jar.write(classes / member, member)
        (source / "RcPackagedSmoke.java").write_text(runtime.PROBE)
        subprocess.run([str(java / "javac"), "--release", "17", "-d", str(probe),
                        str(source / "RcPackagedSmoke.java")], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        native = self.root / "bundled-native.so"
        native.write_bytes(b"synthetic already-loaded native fixture")
        proof = self.root / "proof.json"
        command = [str(java / "java"), "-Dos.arch=amd64", "-Drc.arch=amd64",
                   "-Drc.core=" + str(core), "-Dfake.native.file=" + str(native),
                   "-Drc.native.sha256=" + runtime.digest(native), "-Drc.proof=" + str(proof)]
        cp = ["-cp", os.pathsep.join(map(str, [core, runner, probe])), "RcPackagedSmoke"]
        result = subprocess.run(command + cp, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(proof.read_text())["loaded_native_sha256"], runtime.digest(native))
        for extra in [["-Drc.native.sha256=" + "0" * 64],
                      ["-Drc.core=" + str(runner)], ["-Dfake.tests.fail=true"],
                      ["-Dfake.spark.version=4.2.0"]]:
            proof.unlink()
            result = subprocess.run(command + extra + cp, capture_output=True,
                                    text=True, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(proof.exists())
            # Leave a marker only so the next iteration removes a task-owned file.
            proof.write_text("{}")


if __name__ == "__main__":
    unittest.main()
