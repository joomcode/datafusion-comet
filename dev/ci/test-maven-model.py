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
"""Failure-injection coverage for the model-only Maven startup retry."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(os.environ.get("COMET_MAVEN_MODEL_SCRIPT",
    Path(__file__).resolve().with_name("prepare-maven-model.sh")))


class MavenModelTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="maven-model-test-")
        self.root = Path(self.tmp.name)
        scripts = self.root / "dev/ci"
        scripts.mkdir(parents=True)
        shutil.copyfile(SCRIPT, scripts / SCRIPT.name)
        (self.root / "logs").mkdir()
        (self.root / "bin").mkdir()
        sleep = self.root / "bin/sleep"
        sleep.write_text("#!/bin/sh\nexit 0\n")
        sleep.chmod(0o755)
        wrapper = self.root / "mvnw"
        wrapper.write_text("""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
log = Path('calls.json')
calls = json.loads(log.read_text()) if log.exists() else []
calls.append(sys.argv[1:])
log.write_text(json.dumps(calls))
mode = os.environ['MODEL_TEST_MODE']
if mode in ('recover', 'known-root-recover') and len(calls) > 1:
 print('[INFO] BUILD SUCCESS'); sys.exit(0)
if mode == 'invalid':
 print('[ERROR] Non-parseable POM'); sys.exit(17)
if mode == 'enforcer':
 print('[INFO] Building parent 1.2.0')
 print('[ERROR] Enforcer rule failed'); sys.exit(17)
if mode.startswith('known-root') or mode in ('wrong-root', 'wrong-coordinate', 'wrong-url'):
 root = 'org.apache.datafusion:comet-parent-spark${spark.version.short}_${scala.binary.version}:1.2.0'
 coordinate = 'org.apache:apache:pom:23'
 url = 'https://repo.maven.apache.org/maven2'
 if mode == 'wrong-root': root = 'org.example:dependency-parent:1'
 if mode == 'wrong-coordinate': coordinate = 'org.apache:apache:pom:24'
 if mode == 'wrong-url': url = 'https://mirror.example.invalid/maven2'
 if mode == 'known-root-started': print('[INFO] Building parent 1.2.0')
 print('[FATAL] Non-resolvable parent POM for ' + root + ': The following artifacts could not '
       + 'be resolved: ' + coordinate + ' (absent): Could not find artifact ' + coordinate
       + ' in central (' + url + ") and 'parent.relativePath' points at wrong local POM")
 sys.exit(17)
if mode == 'started':
 print('[INFO] Building parent 1.2.0')
code = '404' if mode == 'not-found' else '429'
print('[FATAL] Non-resolvable parent POM: Could not transfer artifact parent:pom:1 '
      + 'status code: ' + code + ', reason phrase: Unavailable')
sys.exit(17)
""")
        wrapper.chmod(0o755)

    def tearDown(self):
        self.tmp.cleanup()

    def run_model(self, mode, *args):
        env = dict(os.environ, MODEL_TEST_MODE=mode,
                   RUNNER_TEMP=str(self.root / "logs"),
                   PATH=str(self.root / "bin") + os.pathsep + os.environ["PATH"])
        result = subprocess.run(["bash", str(self.root / "dev/ci" / SCRIPT.name), *args],
                                cwd=self.root, env=env, capture_output=True, text=True)
        calls = self.root / "calls.json"
        return result, json.loads(calls.read_text()) if calls.exists() else []

    def test_transient_parent_failure_recovers_only_model(self):
        result, calls = self.run_model("recover", "-Pspark-4.2", "-Dexample=value")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(calls), 2)
        for call in calls:
            self.assertEqual(call, ["-B", "-N", "-U", "-Dstyle.color=never",
                                    "-Pspark-4.2", "-Dexample=value", "validate"])

    def test_retry_budget_is_bounded_and_preserves_failure(self):
        result, calls = self.run_model("exhaust")
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 3)

    def test_known_immutable_root_lookup_recovers_only_model(self):
        result, calls = self.run_model("known-root-recover", "-Prelease", "-DskipTests",
                                       "-Pspark-3.4", "-Pscala-2.13")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(calls), 2)
        for call in calls:
            self.assertEqual(call, ["-B", "-N", "-U", "-Dstyle.color=never", "-Prelease",
                                    "-DskipTests", "-Pspark-3.4", "-Pscala-2.13", "validate"])

    def test_known_root_lookup_exhaustion_preserves_failure(self):
        result, calls = self.run_model("known-root-exhaust")
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 3)

    def test_known_root_classifier_rejects_other_roots_coordinates_and_repositories(self):
        for mode in ("wrong-root", "wrong-coordinate", "wrong-url", "known-root-started"):
            with self.subTest(mode=mode):
                (self.root / "calls.json").unlink(missing_ok=True)
                result, calls = self.run_model(mode)
                self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
                self.assertEqual(len(calls), 1)

    def test_log_pipeline_failure_is_not_retried_or_hidden(self):
        tee = self.root / "bin/tee"
        tee.write_text("#!/bin/sh\ncat > /dev/null\nexit 19\n")
        tee.chmod(0o755)
        result, calls = self.run_model("known-root-exhaust")
        self.assertEqual(result.returncode, 19, result.stdout + result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertIn("Could not record Maven model output", result.stderr)

    def test_other_failures_are_not_retried(self):
        for mode in ("not-found", "invalid", "enforcer", "started"):
            with self.subTest(mode=mode):
                calls_path = self.root / "calls.json"
                calls_path.unlink(missing_ok=True)
                result, calls = self.run_model(mode)
                self.assertEqual(result.returncode, 17)
                self.assertEqual(len(calls), 1)

    def test_compile_or_test_goals_are_rejected(self):
        for goal in ("compile", "test", "install", "help:effective-pom"):
            with self.subTest(goal=goal):
                result, calls = self.run_model("recover", goal)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
