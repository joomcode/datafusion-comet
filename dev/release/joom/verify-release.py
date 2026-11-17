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

"""Validate local Joom release bytes; no network or credentials are used."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate(out, commit, lock_sha256):
    core = "comet-spark-spark4.2_2.13-1.2.0.jar"
    delta = "comet-contrib-delta-spark4.2_2.13-1.2.0.jar"
    expected = {
        core, delta, "linux/aarch64/libcomet.so", "linux/amd64/libcomet.so",
        "commit.txt", "build-manifest.json", "effective-pom.xml",
    }
    expected.update(f"linux/{arch}/{name}" for arch in ["aarch64", "amd64"]
                    for name in ["cargo-metadata.json", "toolchain.json", "elf-imports.txt"])
    hashes = {}
    for line in (out / "sha256.txt").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        require(match is not None, "invalid checksum record")
        digest, name = match.groups()
        require(name in expected and name not in hashes, "unexpected/duplicate checksum path")
        hashes[name] = digest
        require(hashlib.sha256((out / name).read_bytes()).hexdigest() == digest,
                f"checksum mismatch: {name}")
    require(set(hashes) == expected, "incomplete checksum manifest")
    require((out / "commit.txt").read_text().strip() == commit, "build commit mismatch")
    manifest = json.loads((out / "build-manifest.json").read_text())
    require(manifest["commit"] == commit, "provenance commit mismatch")
    require(manifest["cargo_lock_sha256"] == lock_sha256, "provenance lockfile mismatch")
    require(manifest["versions"] == {
        "comet": "1.2.0", "spark": "4.2.1", "scala": "2.13.18", "delta": "4.4.1",
        "datafusion": "55.2.0", "arrow_parquet": "59.3.2", "rust": "1.100.0", "java": "17",
    }, "unexpected release versions")
    require(manifest["max_glibc"] == "2.31", "unexpected GLIBC ceiling")
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    pom = ET.parse(out / "effective-pom.xml").getroot()
    project = pom if pom.tag.endswith("}project") else pom.find("m:project", ns)
    props = project.find("m:properties", ns)
    for key, value in {"spark.version": "4.2.1", "scala.version": "2.13.18",
                       "delta.spark.version": "4.4.1", "delta.artifact": "delta-spark_4.2"}.items():
        require(props.find("m:" + key, ns).text == value, f"effective POM mismatch: {key}")
    for arch, target, flags in [("aarch64", "aarch64-unknown-linux-gnu", ""),
                                ("amd64", "x86_64-unknown-linux-gnu", "-Ctarget-cpu=x86-64-v3")]:
        directory = out / "linux" / arch
        graph = json.loads((directory / "cargo-metadata.json").read_text())
        packages = graph["packages"]
        physical = [p for p in packages if p["name"] == "datafusion-physical-plan"]
        require(len(physical) == 1 and physical[0]["source"] is None
                and physical[0]["version"] == "55.2.0"
                and physical[0]["manifest_path"].endswith("/native/vendor/datafusion-physical-plan/Cargo.toml"),
                f"wrong physical-plan graph: {arch}")
        for package in packages:
            name, version = package["name"], package["version"]
            if name == "datafusion" or (name.startswith("datafusion-") and not name.startswith("datafusion-comet")):
                require(version == "55.2.0", f"DataFusion version drift: {name}")
            if name in ["arrow", "parquet"] or name.startswith("arrow-"):
                require(version == "59.3.2", f"Arrow/Parquet version drift: {name}")
        tool = json.loads((directory / "toolchain.json").read_text())
        require("release: 1.100.0" in tool["rustc"].splitlines()
                and f"host: {target}" in tool["rustc"].splitlines(), f"wrong Rust target/version: {arch}")
        require(tool["cargo"].split()[1] == "1.100.0", f"wrong Cargo version: {arch}")
        java = tuple(map(int, tool["javac"].split()[1].split(".")))
        require(java[0] == 17 and java >= (17, 0, 11), f"unsupported Java: {arch}")
        require(tool["gcc"].startswith("10.") and tool["protoc"] == "libprotoc 30.2",
                f"unexpected compiler tools: {arch}")
        require(tool["rustflags"] == flags and manifest["native_flags"][arch] == flags,
                f"native flags mismatch: {arch}")
        imports = [line for line in (directory / "elf-imports.txt").read_text().splitlines()
                   if "*UND*" in line]
        required = set(re.findall(r"\bGLIBC_([A-Za-z0-9_.]+)\b", "\n".join(imports)))
        require(required, f"missing ELF GLIBC import requirements: {arch}")
        require(all(re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", v) for v in required),
                f"unsupported named GLIBC requirement: {arch}")
        require(max(tuple(map(int, v.split("."))) for v in required) <= (2, 31),
                f"GLIBC > 2.31: {arch}")
    with zipfile.ZipFile(out / core) as jar:
        names = jar.namelist()
        require(len(names) == len(set(names)), "duplicate core JAR entries")
        git_properties = dict(line.split("=", 1) for line in
                              jar.read("comet-git-info.properties").decode().splitlines()
                              if line and not line.startswith("#") and "=" in line)
        require(git_properties.get("git.commit.id.full") == commit, "embedded Git commit mismatch")
        native = [n for n in names if n.endswith(("libcomet.so", "libcomet.dylib", "comet.dll"))]
        require(set(native) == {
            "org/apache/comet/linux/aarch64/libcomet.so",
            "org/apache/comet/linux/amd64/libcomet.so",
        }, "expected exactly two Linux native libraries")
        for arch, machine in [("aarch64", 183), ("amd64", 62)]:
            lib = (out / f"linux/{arch}/libcomet.so").read_bytes()
            require(jar.read(f"org/apache/comet/linux/{arch}/libcomet.so") == lib,
                    f"packaged native bytes mismatch: {arch}")
            require(lib[:5] == b"\x7fELF\x02" and lib[5] in [1, 2], "expected ELF64")
            order = "little" if lib[5] == 1 else "big"
            require(int.from_bytes(lib[18:20], order) == machine, f"wrong ELF machine: {arch}")
    with zipfile.ZipFile(out / delta) as jar:
        names = jar.namelist()
        require(len(names) == len(set(names)), "duplicate Delta JAR entries")
        require(any(n.endswith("/DeltaScanContrib.class") for n in names), "missing Delta contrib")
        providers = {
            "org.apache.spark.sql.comet.PlanDataInjector":
                "org.apache.spark.sql.comet.DeltaPlanDataInjector",
            "org.apache.comet.rules.CometScanContrib":
                "org.apache.comet.contrib.delta.DeltaScanContrib",
            "org.apache.comet.CometConfigProvider":
                "org.apache.comet.contrib.delta.DeltaSparkConfigProvider",
        }
        for service, provider in providers.items():
            lines = jar.read("META-INF/services/" + service).decode("utf-8").splitlines()
            actual = [line.split("#", 1)[0].strip() for line in lines]
            require([line for line in actual if line] == [provider], f"wrong SPI: {service}")
            require(provider.replace(".", "/") + ".class" in names,
                    f"missing SPI implementation: {provider}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--lock-sha256", required=True)
    args = parser.parse_args()
    try:
        validate(args.out, args.commit, args.lock_sha256)
    except (ValueError, OSError, KeyError, AttributeError, IndexError, ET.ParseError, zipfile.BadZipFile) as error:
        parser.exit(1, f"Release validation failed: {error}\n")
    print(f"Validated local release: {args.commit}")


if __name__ == "__main__":
    main()
