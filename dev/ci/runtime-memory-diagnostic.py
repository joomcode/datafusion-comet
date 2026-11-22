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
"""Linux memory diagnosis only; no test, CI or release acceptance credit."""
import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import threading
import time
import xml.etree.ElementTree as ET

BASE = "d00c8fdd3c05ee919c1ade656647d4571983d03f"
ALLOWED = {"dev/ci/runtime-memory-diagnostic.py", ".github/workflows/runtime_memory_diagnostics.yml"}
SUITE = "org.apache.comet.exec.CometSmjJoinFilterFuzzSuite"
CASE = "full: groups of thousands of rows, boolean and conditional operators, batch1024"
STOP = threading.Event()
JAVA_IDENTITIES = {}


def read(path):
    try:
        return Path(path).read_text(errors="replace")
    except (OSError, ProcessLookupError):
        return ""


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as src:
        for block in iter(lambda: src.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def git(repo, *args):
    return subprocess.check_output(["git"] + list(args), cwd=str(repo), text=True).strip()


def source_origin(repo, out):
    if git(repo, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Tracked files are dirty; refuse a source-ambiguous diagnostic")
    changed = git(repo, "diff", "--name-only", BASE, "HEAD").splitlines()
    if not set(changed).issubset(ALLOWED):
        raise ValueError("Diagnostic branch changes non-diagnostic source: " + repr(changed))
    paths = git(repo, "ls-files").splitlines()
    source = {p: digest(repo / p) for p in paths if p.endswith(
        (".scala", ".java", ".rs", ".proto", ".toml", ".lock", ".xml", ".py", ".yml", ".yaml"))}
    libraries = sorted((repo / "native/target/release").glob("*.so"))
    if not any(p.name == "libcomet.so" and p.stat().st_size for p in libraries):
        raise ValueError("The pinned native libcomet.so was not downloaded")
    save(out / "source-origin.json", {
        "baseline_commit": BASE, "diagnostic_commit": git(repo, "rev-parse", "HEAD"),
        "changed_paths": changed, "source_sha256": source,
        "native_sha256": {str(p.relative_to(repo)): digest(p) for p in libraries},
        "controller_sha256": digest(__file__), "qualification_credit": False})


def classpath_origin(repo, out, required=False):
    reports = repo / "spark/target/surefire-reports"
    choices = [reports / ("TEST-" + s + ".xml") for s in
               ("org.apache.comet.CometConfSuite", "org.apache.comet.exec.CometAggregateSuite")]
    xml = next((p for p in choices if p.is_file()), None)
    if xml is None:
        if required:
            raise ValueError("No completed bootstrap suite XML for classpath")
        save(out / "classpath-origin.json", {"status": "NO_COMPLETED_SUITE_XML", "qualification_credit": False})
        return None
    props = {p.get("name"): p.get("value") for p in ET.parse(xml).findall("./properties/property")}
    cp = props["java.class.path"]
    if "/spark-sql_2.13/4.2.1/" not in cp or "/scala-library/2.13.18/" not in cp:
        raise ValueError("XML classpath is not the exact Spark4.2.1/Scala2.13.18 target")
    entries = []
    for raw in cp.split(os.pathsep):
        p = Path(raw)
        if not p.is_absolute() or not p.exists():
            raise ValueError("Missing/non-absolute classpath entry: " + raw)
        row = {"path": raw}
        if p.is_file():
            row.update(bytes=p.stat().st_size, sha256=digest(p))
        else:
            row["class_sha256"] = {str(f.relative_to(p)): digest(f) for f in sorted(p.rglob("*.class"))}
        entries.append(row)
    save(out / "classpath-origin.json", {"XML": str(xml), "XML_sha256": digest(xml),
         "entries": entries, "qualification_credit": False})
    return cp


def java_identity(directory):
    stat = read(directory / "stat").rsplit(")", 1)[-1].split()
    start = stat[19] if len(stat) > 19 else None
    key = (directory.name, start)
    if key not in JAVA_IDENTITIES:
        try:
            with (directory / "cmdline").open("rb") as src:
                raw = src.read(262145)
        except OSError:
            raw = b""
        argv = raw[:262144].decode(errors="replace").split("\0")
        main, skip = None, False
        for arg in argv[1:]:
            if skip:
                skip = False
            elif arg in ("-cp", "-classpath", "--class-path", "-p", "--module-path"):
                skip = True
            elif arg and not arg.startswith("-"):
                main = Path(arg).name if arg.endswith(".jar") else arg
                break
        role = ("maven" if main and "plexus.classworlds" in main else
                "scalatest" if main and main.startswith("org.scalatest.") else
                "compiler" if main and "scala_maven" in main else "other-java")
        JAVA_IDENTITIES[key] = {"start_ticks": start, "main": main, "role": role,
                                "cmdline_prefix_sha256": hashlib.sha256(raw[:262144]).hexdigest(),
                                "cmdline_truncated": len(raw) > 262144}
    return JAVA_IDENTITIES[key]


def resources():
    result = {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "java": [], "other": []}
    for d in Path("/proc").iterdir():
        if not d.name.isdigit():
            continue
        fields = dict(line.split(":", 1) for line in read(d / "status").splitlines() if ":" in line)
        if "VmRSS" not in fields:
            continue
        row = {"pid": int(d.name)}
        row.update({k: fields[k].strip() for k in ("Name", "PPid", "VmRSS", "VmHWM", "VmSize", "RssAnon", "RssFile", "RssShmem", "VmSwap", "Threads") if k in fields})
        if fields.get("Name", "").strip() == "java":
            row["identity"] = java_identity(d)
            row["smaps_rollup"] = read(d / "smaps_rollup")
            result["java"].append(row)
        else:
            result["other"].append(row)
    result["cgroup"] = {n: read(Path("/sys/fs/cgroup") / n) for n in
                        ("memory.current", "memory.peak", "memory.max", "memory.events", "memory.stat")}
    result["meminfo"] = read("/proc/meminfo")
    return result


def probe(pids, out, tick):
    jcmd = str(Path(os.environ["JAVA_HOME"]) / "bin/jcmd")
    for pid in pids:
        for command in ("GC.heap_info", "VM.native_memory summary"):
            if STOP.is_set():
                return
            path = out / ("jcmd-%06d-%s-%s.log" % (tick, pid, command.split()[0]))
            with path.open("w") as log:
                log.write("command: jcmd %s %s\n" % (pid, command))
                log.flush()
                try:
                    result = subprocess.run([jcmd, str(pid)] + command.split(), stdout=log,
                                            stderr=subprocess.STDOUT, timeout=10)
                    log.write("\nexit_code: %d\n" % result.returncode)
                except (OSError, subprocess.TimeoutExpired) as error:
                    log.write("\nprobe_error: %s\n" % error)


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    until = time.monotonic() + 5
    while time.monotonic() < until:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def isolated_command(repo, out):
    cp = classpath_origin(repo, out, required=True)
    root = ET.parse(repo / "pom.xml").getroot()
    extra = root.findtext("./{*}properties/{*}extraJavaTestArgs")
    args = shlex.split(extra)
    if any("$" in x for x in args):
        raise ValueError("Unresolved Maven Java test options")
    (out / "reports").mkdir(exist_ok=True)
    (out / "tmp").mkdir(exist_ok=True)
    return [str(Path(os.environ["JAVA_HOME"]) / "bin/java"), "-ea", "-Xmx4g", "-Xss4m"] + args + [
        "-Djava.awt.headless=true", "-Djava.io.tmpdir=" + str(out / "tmp"),
        "-Dlog4j.configurationFile=file:" + str(repo / "spark/src/test/resources/log4j2.properties"),
        "-Dspark.hadoop.fs.s3a.committer.magic.enabled=false", "-cp", cp,
        "org.scalatest.tools.Runner", "-R", str(repo / "spark/target/test-classes"),
        "-s", SUITE, "-t", CASE, "-oD", "-u", str(out / "reports"), "-f", str(out / "ScalaTest.txt")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout", required=True, type=int)
    parser.add_argument("mode", choices=("wrap", "isolated"))
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    out = args.output.resolve()
    out.relative_to(repo / "target/memory-diagnostic")
    if out.exists() and any(out.iterdir()):
        raise FileExistsError("Preserve the existing diagnostic output: " + str(out))
    out.mkdir(parents=True, exist_ok=True)
    source_origin(repo, out)
    command = isolated_command(repo, out) if args.mode == "isolated" else args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command or not 1 <= args.timeout <= 10500:
        raise ValueError("Supply a command and a bounded positive timeout")
    receipt = {"command": command, "mode": args.mode, "timeout_seconds": args.timeout,
               "source_commit": git(repo, "rev-parse", "HEAD"), "qualification_credit": False,
               "JAVA_TOOL_OPTIONS": os.environ.get("JAVA_TOOL_OPTIONS"),
               "MAVEN_OPTS": os.environ.get("MAVEN_OPTS"), "status": "RUNNING"}
    save(out / "result.json", receipt)
    interrupted = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda number, frame: interrupted.append(number))
    start = time.monotonic()
    next_sample = next_probe = 0
    timed_out = False
    future = None
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    before = resources()
    save(out / "resources-before.json", before)
    try:
        with (out / "command.log").open("xb") as log, (out / "samples.jsonl").open("x") as samples:
            process = subprocess.Popen(command, cwd=str(repo / "spark" if args.mode == "isolated" else repo),
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            receipt["process_group"] = process.pid
            save(out / "result.json", receipt)
            while process.poll() is None:
                elapsed = time.monotonic() - start
                if elapsed >= next_sample:
                    row = resources()
                    row["elapsed_seconds"] = round(elapsed, 2)
                    samples.write(json.dumps(row) + "\n")
                    samples.flush()
                    print(json.dumps({"elapsed": round(elapsed), "java_pids": [p["pid"] for p in row["java"]],
                                      "java_rss": [p["VmRSS"] for p in row["java"]],
                                      "cgroup_current": row["cgroup"]["memory.current"].strip()}), flush=True)
                    next_sample = elapsed + 10
                    if elapsed >= next_probe and (future is None or future.done()):
                        future = pool.submit(probe, [p["pid"] for p in row["java"]], out, int(elapsed))
                        next_probe = elapsed + 60
                if elapsed >= args.timeout or interrupted:
                    timed_out = elapsed >= args.timeout
                    break
                time.sleep(0.5)
    finally:
        STOP.set()
        if "process" in locals():
            stop_group(process)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                receipt["cleanup_error"] = "SIGKILL sent; process did not reap within ten seconds"
        pool.shutdown(wait=True)
        after = resources()
        save(out / "resources-after.json", after)
    command_code = (125 if process.returncode is None else process.returncode
                    if process.returncode >= 0 else 128 - process.returncode)
    code = 124 if timed_out else 128 + interrupted[0] if interrupted else command_code
    receipt.update(status="DIAGNOSTIC_FINISHED_NOT_QUALIFICATION", command_returncode=process.returncode,
                   exit_code=code, timed_out=timed_out, interrupted_signals=interrupted,
                   elapsed_seconds=round(time.monotonic() - start, 2))
    try:
        classpath_origin(repo, out)
    except (OSError, ValueError, KeyError, ET.ParseError) as error:
        receipt["classpath_origin_error"] = str(error)
        if code == 0:
            code = 2
            receipt["exit_code"] = code
    save(out / "result.json", receipt)
    print(json.dumps({"exit_code": code, "result": str(out / "result.json"), "qualification_credit": False}), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
