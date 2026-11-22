#!/usr/bin/env python3
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
"""Standalone official-Spark Parquet ownership diagnosis; no qualification credit."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

BASE = "d00c8fdd3c05ee919c1ade656647d4571983d03f"
ALLOWED = {".github/workflows/runtime_memory_diagnostics.yml",
           "dev/ci/runtime-memory-diagnostic.py", "dev/ci/SparkParquetMemoryProbe.java"}
MAIN = "org.apache.spark.sql.execution.datasources.parquet.SparkParquetMemoryProbe"
POM = """<project xmlns="http://maven.apache.org/POM/4.0.0"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
 <modelVersion>4.0.0</modelVersion><groupId>org.apache.comet.diagnostic</groupId>
 <artifactId>official-spark-parquet-memory-probe</artifactId><version>1</version>
 <dependencies><dependency><groupId>org.apache.spark</groupId>
 <artifactId>spark-sql_2.13</artifactId><version>4.2.1</version>
 </dependency></dependencies>
</project>
"""


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def git(root, *args):
    return subprocess.check_output(["git"] + list(args), cwd=str(root), text=True).strip()


def run(command, output, name, cwd, env, timeout):
    before = time.monotonic()
    with (output / (name + ".log")).open("w") as log:
        process = subprocess.run(command, cwd=str(cwd), env=env,
                                 stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
    save(output / (name + "-result.json"), {
        "argv": command, "exit_code": process.returncode,
        "seconds": time.monotonic() - before, "qualification_credit": False})
    if process.returncode:
        raise RuntimeError(name + " failed; see its explicit log")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output = Path(args.output).resolve()
    try:
        output.relative_to(root / "target")
    except ValueError:
        raise ValueError("Output must be an explicit project target subdirectory")
    output.mkdir(parents=True, exist_ok=True)
    if git(root, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Refuse dirty tracked source")
    changed = git(root, "diff", "--name-only", BASE, "HEAD").splitlines()
    if not set(changed).issubset(ALLOWED):
        raise ValueError("Non-diagnostic source changed: " + repr(changed))
    source = root / "dev/ci/SparkParquetMemoryProbe.java"
    save(output / "source-origin.json", {
        "baseline_commit": BASE, "diagnostic_commit": git(root, "rev-parse", "HEAD"),
        "changed_paths": changed, "qualification_credit": False,
        "probe_sha256": sha(source), "controller_sha256": sha(__file__),
        "workflow_sha256": sha(root / ".github/workflows/runtime_memory_diagnostics.yml"),
        "utc": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    (output / "pom.xml").write_text(POM)
    (output / "tmp").mkdir(exist_ok=True)
    (output / "classes").mkdir(exist_ok=True)
    environment = dict(os.environ, TMPDIR=str(output / "tmp"))
    # Standalone POM has no Comet parent, reactor, dependencies, native library or test classes.
    run([str(root / "mvnw"), "-B", "-f", str(output / "pom.xml"),
         "org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath",
         "-Dmdep.outputFile=" + str(output / "dependency-classpath.txt")],
        output, "dependency-classpath", output, environment, 600)
    classpath = (output / "dependency-classpath.txt").read_text().strip()
    entries = []
    for value in classpath.split(os.pathsep):
        path = Path(value)
        if not path.is_absolute() or not path.is_file() or path.suffix != ".jar":
            raise ValueError("Dependency classpath entry is not an absolute JAR: " + value)
        if "comet" in path.name.lower():
            raise ValueError("Comet must not be in this official-Spark-only classpath")
        entries.append({"path": str(path), "bytes": path.stat().st_size, "sha256": sha(path)})
    if sum(Path(row["path"]).name == "spark-sql_2.13-4.2.1.jar" for row in entries) != 1:
        raise ValueError("Expected exactly one official Spark SQL4.2.1 JAR")
    save(output / "classpath-origin.json", {"entries": entries, "qualification_credit": False})
    run(["javac", "-classpath", classpath, "-d", str(output / "classes"), str(source)],
        output, "javac", output, environment, 60)
    run(["java", "-Xms64m", "-Xmx256m", "-XX:NativeMemoryTracking=summary",
         "-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=" + str(output / "tmp"),
         "-classpath", str(output / "classes") + os.pathsep + classpath, MAIN, str(output)],
        output, "probe", output, environment, 60)
    receipt = json.loads((output / "ownership-witness.json").read_text())
    if receipt.get("status") != "OFFICIAL_SPARK_AUXILIARY_OWNERSHIP_DEFECT_REPRODUCED":
        raise ValueError("Actual ownership witness did not complete")
    save(output / "diagnostic-result.json", {
        "qualification_credit": False, "status": receipt["status"],
        "ownership_witness_sha256": sha(output / "ownership-witness.json"),
        "classpath_origin_sha256": sha(output / "classpath-origin.json"),
        "source_origin_sha256": sha(output / "source-origin.json"),
        "limits": ["Zero rows; no SparkContext, SQL or Comet execution.",
                   "Demonstrates actual official Parquet-reader cleanup, not the full warmed OOM share."]})
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as failure:
        print(type(failure).__name__ + ": " + str(failure), file=sys.stderr)
        raise
