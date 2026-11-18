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

"""Exercise cross-job native input corruption and provenance rejection without building."""

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

MODULE = Path(__file__).with_name("native-build-provenance.py")
spec = importlib.util.spec_from_file_location("native_provenance", MODULE)
provenance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provenance)
COMMIT = "a" * 40
LOCK = "b" * 64
IMAGE = "sha256:" + "c" * 64
WORK_DIR = None


class NativeStageInputs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="native-stage-", dir=WORK_DIR)
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)
        self.proofs = {}
        for arch, flags in provenance.FLAGS.items():
            directory = self.out / "linux" / arch
            directory.mkdir(parents=True)
            (directory / "libcomet.so").write_bytes(b"native-" + arch.encode())
            (directory / "cargo-metadata.json").write_text('{"packages": []}')
            (directory / "toolchain.json").write_text(json.dumps({"rustflags": flags}))
            (directory / "elf-imports.txt").write_text("fixture\n")
            self.proofs[arch] = provenance.record(self.out, arch, COMMIT, LOCK, IMAGE, 4)

    def change(self, **fields):
        proof = copy.deepcopy(self.proofs["amd64"])
        proof.update(fields)
        (self.out / "linux/amd64/build-provenance.json").write_text(json.dumps(proof))

    def test_both_matching_inputs_accepted(self):
        for arch in provenance.FLAGS:
            self.assertEqual(provenance.validate(self.out, arch, COMMIT, LOCK), self.proofs[arch])

    def test_library_tampering_rejected(self):
        (self.out / "linux/amd64/libcomet.so").write_bytes(b"wrong library")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)

    def test_cross_commit_library_rejected(self):
        self.change(source_commit="d" * 40)
        with self.assertRaisesRegex(ValueError, "commit mismatch"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)

    def test_cross_lockfile_library_rejected(self):
        self.change(cargo_lock_sha256="d" * 64)
        with self.assertRaisesRegex(ValueError, "lockfile mismatch"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)

    def test_wrong_architecture_rejected(self):
        self.change(architecture="aarch64")
        with self.assertRaisesRegex(ValueError, "identity"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)

    def test_ci_profile_rejected(self):
        self.change(native_profile="ci")
        with self.assertRaisesRegex(ValueError, "Cargo profile"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)

    def test_changed_toolchain_rejected(self):
        (self.out / "linux/amd64/toolchain.json").write_text('{"rustflags": ""}')
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)

    def test_missing_stage_component_rejected(self):
        self.change(files={"libcomet.so": self.proofs["amd64"]["files"]["libcomet.so"]})
        with self.assertRaisesRegex(ValueError, "incomplete"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)

    def test_symlink_input_rejected(self):
        path = self.out / "linux/amd64/libcomet.so"
        path.unlink()
        path.symlink_to(self.out / "linux/aarch64/libcomet.so")
        with self.assertRaisesRegex(ValueError, "symlinked"):
            provenance.validate(self.out, "amd64", COMMIT, LOCK)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True,
                        help="explicit existing directory for disposable test fixtures")
    args, remaining = parser.parse_known_args()
    WORK_DIR = args.work_dir
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    unittest.main(argv=[str(Path(__file__))] + remaining)
