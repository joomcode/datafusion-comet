#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Verify the actual production-qualification JVM, including its vendor and build."""
import argparse
import json
import os
import re
import subprocess
from pathlib import Path

POLICY = json.loads(Path(__file__).with_name("java25-runtime.json").read_text())


def validate(proof):
    expected = {
        "java_version": POLICY["java_version"],
        "java_runtime_version": POLICY["runtime_version"],
        "java_vendor": POLICY["vendor"],
    }
    if any(proof.get(key) != value for key, value in expected.items()):
        raise ValueError("JVM does not match the pinned Temurin qualification runtime: "
                         + json.dumps({key: proof.get(key) for key in expected}, sort_keys=True))
    parts = tuple(int(part) for part in proof["java_version"].split("."))
    if parts[0] != POLICY["feature"] or parts[2] < POLICY["minimum_update"]:
        raise ValueError("Spark 4.2.1 requires Java 25.0.3 or newer within Java 25")
    return proof


def inspect(java):
    result = subprocess.run([str(java), "-XshowSettings:properties", "-version"],
                            capture_output=True, text=True, check=True)
    properties = dict(re.findall(r"^\s+([\w.]+) = (.*)$", result.stderr, re.MULTILINE))
    return validate({
        "java_version": properties.get("java.version"),
        "java_runtime_version": properties.get("java.runtime.version"),
        "java_vendor": properties.get("java.vendor"),
        "architecture": properties.get("os.arch"),
        "java_home": properties.get("java.home"),
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", type=Path,
                        default=Path(os.environ.get("JAVA_HOME", "")) / "bin/java")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--github-env", type=Path)
    args = parser.parse_args()
    proof = inspect(args.java)
    text = json.dumps(proof, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    if args.github_env:
        # Preserve the caller's module opens, full-fuzz switches and Spark SQL
        # defaults. JAVA_TOOL_OPTIONS also reaches JNI-created JVMs, unlike
        # launcher-only JDK_JAVA_OPTIONS.
        options = " ".join([os.environ.get("JAVA_TOOL_OPTIONS", "").strip()]
                           + POLICY["vm_options"]).strip()
        with args.github_env.open("a") as env:
            env.write("JAVA_TOOL_OPTIONS=" + options + "\n")
    print(text, end="")


if __name__ == "__main__":
    main()
