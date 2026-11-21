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
                "Tests: succeeded 23, failed 0, canceled 0, ignored 0, pending 0\n")
        runtime.assert_test_summary(good, 23)
        for malformed in [
            good.replace("succeeded 23", "succeeded 22"),
            good.replace("failed 0", "failed 1"),
            good.replace("canceled 0", "canceled 1"),
            good.replace("ignored 0", "ignored 1"),
            good.replace("pending 0", "pending 1"),
            good.replace("aborted 0", "aborted 1"),
            good + good,
            "All tests passed.\n",
        ]:
            with self.subTest(summary=malformed), self.assertRaises(ValueError):
                runtime.assert_test_summary(malformed, 23)

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

    def test_multi_release_support_cannot_hide_production_classes(self):
        member = "META-INF/versions/17/org/apache/comet/udf/codegen/CometUdfFunctionPool$.class"
        with self.assertRaisesRegex(ValueError, "production classes"):
            runtime.check_test_jar(self.jar("multi-release.jar", member))

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
        with zipfile.ZipFile(out / runtime.CORE, "w") as jar:
            jar.writestr("org/apache/comet/FixtureProduction.class", b"final core bytes")
        with zipfile.ZipFile(out / runtime.DELTA, "w") as jar:
            jar.writestr("fixture.txt", b"final delta bytes")
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

    def refresh_manifest(self, out, manifest):
        support = out / "test-support"
        manifest["files"] = {p.relative_to(support).as_posix(): runtime.digest(p)
                             for p in support.rglob("*")
                             if p.is_file() and p.name != "manifest.json"}
        (support / "manifest.json").write_text(json.dumps(manifest))

    def test_support_rejects_unlisted_final_production_classes(self):
        out, manifest = self.fixture()
        with zipfile.ZipFile(out / "test-support/core-tests.jar", "a") as jar:
            jar.writestr("org/apache/comet/FixtureProduction.class", b"foreign main class")
        # Recompute the support checksum so this tests the production-definition
        # guard, rather than failing earlier on an unrelated checksum mismatch.
        self.refresh_manifest(out, manifest)
        with self.assertRaisesRegex(ValueError, "production classes"):
            runtime.validate_support(out, "a" * 40, "b" * 64)

    def test_probe_directory_cannot_supply_production_classes(self):
        out, manifest = self.fixture()
        injected = out / "test-support/probe/org/apache/comet/udf/codegen/CometUdfFunctionPool$.class"
        injected.parent.mkdir(parents=True)
        injected.write_bytes(b"foreign main class")
        self.refresh_manifest(out, manifest)
        with self.assertRaisesRegex(ValueError, "unexpected class or file"):
            runtime.validate_support(out, "a" * 40, "b" * 64)

    def test_every_required_origin_must_be_unique_and_match_final_jar(self):
        hashes = {runtime.CORE: "a" * 64, runtime.DELTA: "b" * 64}
        core = {name: {"jar_name": runtime.CORE, "jar_sha256": hashes[runtime.CORE],
                       "resource_count": 1} for name in runtime.CORE_ORIGIN_CLASSES}
        delta = {name: {"jar_name": runtime.DELTA, "jar_sha256": hashes[runtime.DELTA],
                        "resource_count": 1} for name in runtime.DELTA_ORIGIN_CLASSES}
        runtime.validate_class_origins({"class_origins": core}, hashes, "g08-udf")
        runtime.validate_class_origins({"class_origins": dict(core, **delta)}, hashes,
                                       "g08-delta-serialization")
        pool = "org.apache.comet.udf.codegen.CometUdfFunctionPool$"
        broken = []
        missing = json.loads(json.dumps(core))
        del missing[pool]
        broken.append(missing)
        for key, value in [("jar_name", runtime.DELTA), ("jar_sha256", "c" * 64),
                           ("resource_count", 2), ("resource_count", True)]:
            wrong = json.loads(json.dumps(core))
            wrong[pool][key] = value
            broken.append(wrong)
        broken.append(dict(core, unexpected=core[pool]))
        for origins in broken:
            with self.subTest(origins=origins), self.assertRaises(ValueError):
                runtime.validate_class_origins({"class_origins": origins}, hashes, "g08-udf")
        with self.assertRaises(ValueError):
            runtime.validate_class_origins({}, hashes, "g08-udf")
        with self.assertRaises(ValueError):
            runtime.validate_class_origins({"class_origins": core}, hashes,
                                           "g08-delta-serialization")

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
        for name in runtime.CORE_ORIGIN_CLASSES + runtime.DELTA_ORIGIN_CLASSES:
            if name == "org.apache.comet.NativeBase":
                continue
            package, simple = name.rsplit(".", 1)
            # A post-test provenance inspection must not initialize Scala modules
            # (or any other newly checked definition) after Spark teardown.
            initializer = ("" if name.endswith("CometDeltaNativeScanExec") else
                           'static { if (Boolean.getBoolean("fake.origin.init.fail")) '
                           'throw new IllegalStateException("origin initialized"); }')
            files[simple + ".java"] = ("package " + package + ";\npublic final class "
                                       + simple + " { " + initializer + " }\n")
        for name, text in files.items():
            (source / name).write_text(text)
        subprocess.run([str(java / "javac"), "--release", "17", "-d", str(classes)]
                       + [str(source / name) for name in files], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        core = self.root / runtime.CORE
        with zipfile.ZipFile(core, "w") as jar:
            for name in runtime.CORE_ORIGIN_CLASSES:
                member = name.replace(".", "/") + ".class"
                jar.write(classes / member, member)
        delta = self.root / runtime.DELTA
        with zipfile.ZipFile(delta, "w") as jar:
            for name in runtime.DELTA_ORIGIN_CLASSES:
                member = name.replace(".", "/") + ".class"
                jar.write(classes / member, member)
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
                   "-Drc.core=" + str(core), "-Drc.delta=" + str(delta),
                   "-Dfake.native.file=" + str(native), "-Dfake.origin.init.fail=true",
                   "-Drc.native.sha256=" + runtime.digest(native), "-Drc.proof=" + str(proof)]
        cp = ["-cp", os.pathsep.join(map(str, [core, delta, runner, probe])), "RcPackagedSmoke"]
        result = subprocess.run(command + cp, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        loaded = json.loads(proof.read_text())
        self.assertEqual(loaded["loaded_native_sha256"], runtime.digest(native))
        hashes = {runtime.CORE: runtime.digest(core), runtime.DELTA: runtime.digest(delta)}
        runtime.validate_class_origins(loaded, hashes, "g08-udf")
        result = subprocess.run(command + ["-Drc.require.delta=true"] + cp,
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        runtime.validate_class_origins(json.loads(proof.read_text()), hashes,
                                       "g08-delta-serialization")
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

        pool = "org/apache/comet/udf/codegen/CometUdfFunctionPool$.class"
        foreign = self.root / "foreign.jar"
        with zipfile.ZipFile(foreign, "w") as jar:
            jar.write(classes / pool, pool)
        incomplete = self.root / "incomplete-core.jar"
        with zipfile.ZipFile(incomplete, "w") as target, zipfile.ZipFile(core) as original:
            for member in original.namelist():
                if member != pool:
                    target.writestr(member, original.read(member))
        for origin, jars, error in [
            (core, [core, delta, runner, foreign, probe], "Duplicate org.apache.comet.udf.codegen"),
            (incomplete, [incomplete, delta, runner, foreign, probe],
             "org.apache.comet.udf.codegen.CometUdfFunctionPool$ did not load from final JAR"),
        ]:
            proof.unlink()
            classpath = ["-cp", os.pathsep.join(map(str, jars)), "RcPackagedSmoke"]
            result = subprocess.run(command + ["-Drc.core=" + str(origin)] + classpath,
                                    capture_output=True, text=True, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(error, result.stderr)
            self.assertFalse(proof.exists())
            proof.write_text("{}")


if __name__ == "__main__":
    unittest.main()
