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

"""Bundle matching test support, then smoke the final release JAR bytes offline."""

import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile


# Importing the release validator must not add __pycache__ to the clean checkout.
sys.dont_write_bytecode = True


CORE = "comet-spark-spark4.2_2.13-1.2.0.jar"
DELTA = "comet-contrib-delta-spark4.2_2.13-1.2.0.jar"
CORE_ORIGIN_CLASSES = (
    "org.apache.comet.NativeBase",
    "org.apache.comet.udf.codegen.CometUdfFunctionPool$",
    "org.apache.comet.udf.codegen.CometUdfFunctionPool$PlanPayload",
    "org.apache.comet.udf.codegen.CometSharedFunctionRef",
    "org.apache.comet.udf.codegen.CometSharedFunctionRef$",
    "org.apache.comet.udf.codegen.CometScalaUDFCodegen",
    "org.apache.comet.serde.QueryContextInterner$",
    "org.apache.comet.serde.QueryContextInterner$Pool",
    "org.apache.comet.rules.BoundaryFormats$",
    "org.apache.comet.rules.ChooseBoundaryFormats",
    "org.apache.comet.rules.CostBasedEngineChoice",
    "org.apache.comet.rules.EngineCostModel",
    "org.apache.comet.rules.WideRowSortFallback",
    "org.apache.comet.rules.WideRowShuffleFallback$",
    "org.apache.spark.CometPlugin",
    "org.apache.comet.CometKryoRegistrator",
    "org.apache.spark.sql.comet.CometBroadcastExchangeExec",
    "org.apache.spark.sql.comet.execution.shuffle.NativeShuffleSpec",
    "org.apache.spark.sql.comet.execution.shuffle.NativeShuffleSpec$",
    "org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec",
    "org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec$",
    "org.apache.spark.sql.comet.execution.shuffle.CometNativeShuffleWriter",
    "org.apache.comet.expressions.CometCast",
    "org.apache.comet.expressions.CometCast$",
)
DELTA_ORIGIN_CLASSES = (
    "org.apache.spark.sql.comet.CometDeltaNativeScanExec",
    "org.apache.spark.sql.comet.DeltaPlanDataInjector",
)
DELTA_GROUPS = {"delta", "g08-delta-serialization"}
FORBIDDEN_CLASSES = {name.replace(".", "/") + ".class"
                     for name in CORE_ORIGIN_CLASSES + DELTA_ORIGIN_CLASSES}
CASES = [
    ("delta", "org.apache.comet.contrib.delta.CometDeltaNativeScanSuite", [
        "-t", "plain delta table reads natively with identical results",
        "-t", "deletion vectors: DELETE-produced DVs read natively with correct results",
        "-t", "planning partitioning does not execute a scalar subquery harvested into data filters",
        "-t", "Delta scan copies keep the native plan and Java serialization drops driver-only payloads",
    ], 4),
    ("dpp", "org.apache.spark.sql.comet.CometDppFallbackRepro3949Suite", [
        "-t", "native context preserves zero partitions after file pruning",
    ], 1),
    ("reload", "org.apache.comet.CometSparkSessionExtensionsSuite", [
        "-t", "reloading NativeBase does not load a second copy of the native library",
    ], 1),
    ("ansi", "org.apache.comet.CometExpressionSuite", [
        "-z", "ANSI integral overflow fidelity",
    ], 6),
    ("g08-udf", "org.apache.spark.sql.comet.CometUdfFunctionPoolSuite", [
        "-t", "a real native project shares large UDF captures and retains Spark results",
    ], 1),
    ("g08-delta-serialization", "org.apache.comet.contrib.delta.CometDeltaNativeScanSuite", [
        "-t", "stage task binaries over delta scans do not repeat the query text per expression",
        "-t", "query-text interning preserves Delta opaque payload keys and SQL context positions",
    ], 2),
    ("g09-cost", "org.apache.comet.rules.CostBasedEngineCorrectnessSuite", [
        "-t", "G09 mixed decimal boundaries preserve Spark rows: precision=38 AQE=true",
        "-t", "G09 root scalar/nested decimal shuffles preserve Spark partition ids: 38",
        "-t", "G09 Spark partial buffers feed the retained serialized native final",
        "-t", "G09 cost/boundary choices retain G08 shared CAST mandatory ANSI errors",
    ], 4),
    ("g09-wide-sort", "org.apache.comet.rules.WideRowSortFallbackSuite", [
        "-t", "a sort moved to Spark stays there on repeated runs with boundary formats",
    ], 1),
    ("g09-wide-shuffle", "org.apache.comet.rules.WideRowShuffleFallbackSuite", [
        "-t", "boundary formats keep a wide shuffle in Spark",
    ], 1),
    ("g09-range", "org.apache.comet.exec.CometNativeShuffleSuite", [
        "-t", "wide decimals remain supported in shuffle payloads, ranges and single partitions",
    ], 1),
    ("g07-kryo", "org.apache.spark.sql.comet.CometBroadcastDefaultKryoSuite", [
        "-t", "string distinct broadcast with default Kryo registration: aqe=true",
    ], 1),
    ("g10-udf-shuffle", "org.apache.spark.sql.comet.CometUdfFunctionPoolSuite", [
        "-t", "native hash shuffle transports UDF keys separately from child functions",
        "-t", "native range shuffle transports UDF keys separately from child functions",
    ], 2),
    ("g10-expression-guards", "org.apache.comet.CometExpressionSuite", [
        "-t", "date_add with scalar overflow",
        "-s", "org.apache.comet.CometTimestampComparisonSuite",
        "-t", "unsupported timestamp casts retain Spark rows and mandatory errors through dispatch or fallback",
    ], 2),
]

# Reflection keeps this helper independent of the Scala/ScalaTest ABI at compile time.
# Runner.run returns instead of calling System.exit, so the loaded library can be
# checked while it still exists, before NativeBase's delete-on-exit cleanup runs.
PROBE = r'''
import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

public final class RcPackagedSmoke {
  private static final String[] CORE_CLASSES = {__CORE_CLASSES__};
  private static final String[] DELTA_CLASSES = {__DELTA_CLASSES__};
  private static void require(boolean ok, String message) {
    if (!ok) throw new IllegalStateException(message);
  }
  private static String hash(Path path) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
        .digest(Files.readAllBytes(path)));
  }
  private static Path origin(Class<?> cls) throws Exception {
    return Paths.get(cls.getProtectionDomain().getCodeSource().getLocation().toURI());
  }
  private static String quote(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r") + "\"";
  }
  private static String classOrigin(String name, Path jar, String jarHash) throws Exception {
    ClassLoader loader = ClassLoader.getSystemClassLoader();
    Class<?> cls = Class.forName(name, false, loader);
    require(Files.isSameFile(origin(cls), jar), name + " did not load from final JAR");
    int resources = Collections.list(loader.getResources(name.replace('.', '/') + ".class")).size();
    require(resources == 1, "Duplicate " + name + " on test classpath");
    return quote(name) + ":{\"jar_name\":" + quote(jar.getFileName().toString())
        + ",\"jar_sha256\":" + quote(jarHash) + ",\"resource_count\":" + resources + "}";
  }
  public static void main(String[] args) throws Exception {
    require(Runtime.version().feature() == 17, "Packaged smoke requires Java 17");
    Class<?> sparkPackage = Class.forName("org.apache.spark.package$");
    Object sparkModule = sparkPackage.getField("MODULE$").get(null);
    String sparkVersion = (String) sparkPackage.getMethod("SPARK_VERSION").invoke(sparkModule);
    require(sparkVersion.equals("4.2.1"), "Packaged smoke loaded the wrong Spark version");
    String arch = System.getProperty("rc.arch");
    String jvmArch = System.getProperty("os.arch");
    require(arch.equals("amd64") ? Set.of("amd64", "x86_64").contains(jvmArch)
        : Set.of("aarch64", "arm64").contains(jvmArch), "JVM architecture mismatch");
    Class<?> runner = Class.forName("org.scalatest.tools.Runner");
    boolean passed = (Boolean) runner.getMethod("run", String[].class)
        .invoke(null, (Object) args);
    require(passed, "ScalaTest reported failure");
    Class<?> base = Class.forName("org.apache.comet.NativeBase");
    Path core = Paths.get(System.getProperty("rc.core"));
    require(Files.isSameFile(origin(base), core), "NativeBase did not load from final core JAR");
    Enumeration<java.net.URL> resources = ClassLoader.getSystemClassLoader()
        .getResources("org/apache/comet/NativeBase.class");
    require(Collections.list(resources).size() == 1, "Duplicate NativeBase on test classpath");
    Field loaded = base.getDeclaredField("loaded");
    loaded.setAccessible(true);
    require(loaded.getBoolean(null), "NativeBase reports native library not loaded");
    Field library = base.getDeclaredField("bundledLibrary");
    library.setAccessible(true);
    File extracted = (File) library.get(null);
    require(extracted != null, "NativeBase used a library outside the final JAR");
    String nativeHash = hash(extracted.toPath());
    require(nativeHash.equals(System.getProperty("rc.native.sha256")),
        "Loaded native library differs from the final JAR entry");
    String coreHash = hash(core);
    List<String> classOrigins = new ArrayList<>();
    for (String name : CORE_CLASSES) classOrigins.add(classOrigin(name, core, coreHash));
    if (Boolean.getBoolean("rc.require.delta")) {
      Class<?> delta = Class.forName("org.apache.spark.sql.comet.CometDeltaNativeScanExec");
      Path deltaJar = Paths.get(System.getProperty("rc.delta"));
      require(Files.isSameFile(origin(delta), deltaJar),
          "Delta scan did not load from final Delta JAR");
      String deltaHash = hash(deltaJar);
      for (String name : DELTA_CLASSES) classOrigins.add(classOrigin(name, deltaJar, deltaHash));
    }
    Files.writeString(Paths.get(System.getProperty("rc.proof")),
        "{\"architecture\":" + quote(arch) + ",\"core_jar_sha256\":" + quote(coreHash)
        + ",\"loaded_native_sha256\":" + quote(nativeHash)
        + ",\"loaded_native_path\":" + quote(extracted.toString())
        + ",\"spark_version\":" + quote(sparkVersion)
        + ",\"java_version\":" + quote(System.getProperty("java.version"))
        + ",\"class_origins\":{" + String.join(",", classOrigins) + "}}\n");
  }
}
'''.replace("__CORE_CLASSES__", ", ".join(json.dumps(name) for name in CORE_ORIGIN_CLASSES)) \
    .replace("__DELTA_CLASSES__", ", ".join(json.dumps(name) for name in DELTA_ORIGIN_CLASSES))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def beneath(path, root):
    # The release builder deliberately uses Ubuntu 20.04 / Python 3.8.
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def safe_file(root, name):
    path = Path(name)
    require(not path.is_absolute() and ".." not in path.parts, "unsafe support path")
    resolved = (root / path).resolve()
    require(beneath(resolved, root.resolve()) and resolved.is_file(),
            "missing or escaping support file: " + name)
    return resolved


def release_validator(repo, out, commit, lock):
    path = repo / "dev/release/joom/verify-release.py"
    spec = importlib.util.spec_from_file_location("rc_release_validator", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.validate(out, commit, lock)


def production_classes(out):
    # Derive every Comet/Spark production definition from the final JARs, including
    # nested classes and relocated project dependencies. External test libraries
    # may supply ordinary unshaded third-party dependencies, never project copies.
    classes = set()
    for name in [CORE, DELTA]:
        with zipfile.ZipFile(out / name) as archive:
            classes.update(member for member in archive.namelist()
                           if member.endswith(".class")
                           and member.startswith(("org/apache/comet/", "org/apache/spark/")))
    return classes


def check_test_jar(path, final_classes=()):
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        # A multi-release support JAR must not hide a second production definition.
        definitions = {re.sub(r"^META-INF/versions/[0-9]+/", "", name) for name in names}
        duplicate = (FORBIDDEN_CLASSES | set(final_classes)).intersection(definitions)
        require(not duplicate,
                "test support contains production classes: " + str(path) + ": "
                + ", ".join(sorted(duplicate)))
        require(not any(name.endswith(("libcomet.so", "libcomet.dylib", "comet.dll"))
                        for name in names), "test support contains a native library")


def test_support_pom(effective_pom, classes, target, work):
    # Tests must use the same relocated dependency ABI as the final shaded JARs.
    # A dependency-free project transforms test bytecode only: enabling shadeTestJar
    # in Comet's production reactor would also select dependency test artifacts.
    namespace = "http://maven.apache.org/POM/4.0.0"
    ns = {"m": namespace}
    root = ET.parse(effective_pom).getroot()
    projects = [root] if root.tag == "{" + namespace + "}project" else list(root)
    spark = [project for project in projects
             if project.findtext("m:artifactId", namespaces=ns) == CORE.rsplit("-", 1)[0]]
    require(len(spark) == 1, "effective POM must contain exactly one matching Spark module")
    plugins = spark[0].findall("m:build/m:plugins/m:plugin", ns)

    def plugin(name):
        found = [item for item in plugins if item.findtext("m:artifactId", namespaces=ns) == name]
        require(len(found) == 1, "missing or ambiguous effective plugin: " + name)
        version = found[0].findtext("m:version", namespaces=ns)
        require(version and "${" not in version, "unresolved effective plugin version: " + name)
        return found[0], version

    shade, shade_version = plugin("maven-shade-plugin")
    _, jar_version = plugin("maven-jar-plugin")
    configurations = [execution.find("m:configuration", ns)
                      for execution in shade.findall("m:executions/m:execution", ns)
                      if execution.findtext("m:goals/m:goal", namespaces=ns) == "shade"]
    require(len(configurations) == 1 and configurations[0] is not None,
            "missing or ambiguous effective shade execution")
    relocations = configurations[0].find("m:relocations", ns)
    require(relocations is not None and len(relocations), "missing release relocation policy")
    dependencies = shade.find("m:dependencies", ns)
    require(dependencies is not None and len(dependencies), "missing release shade plugin dependencies")
    for element in [relocations, dependencies]:
        require("${" not in ET.tostring(element, encoding="unicode"),
                "unresolved effective shade policy")
    transformer_name = "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"
    require(any(item.get("implementation") == transformer_name
                for item in configurations[0].findall("m:transformers/m:transformer", ns)),
            "release service-resource transformer missing")

    def add(parent, name, text=None):
        element = ET.SubElement(parent, "{" + namespace + "}" + name)
        element.text = str(text) if text is not None else None
        return element

    ET.register_namespace("", namespace)
    project = ET.Element("{" + namespace + "}project")
    for name, value in [("modelVersion", "4.0.0"), ("groupId", "org.apache.comet.rc"),
                        ("artifactId", "isolated-test-support"), ("version", "1")]:
        add(project, name, value)
    build = add(project, "build")
    add(build, "directory", work / "target")
    generated_plugins = add(build, "plugins")
    generated_jar = add(generated_plugins, "plugin")
    for name, value in [("groupId", "org.apache.maven.plugins"),
                        ("artifactId", "maven-jar-plugin"), ("version", jar_version)]:
        add(generated_jar, name, value)
    add(add(generated_jar, "configuration"), "classesDirectory", classes)
    generated_shade = add(generated_plugins, "plugin")
    for name, value in [("groupId", "org.apache.maven.plugins"),
                        ("artifactId", "maven-shade-plugin"), ("version", shade_version)]:
        add(generated_shade, name, value)
    generated_shade.append(copy.deepcopy(dependencies))
    configuration = add(generated_shade, "configuration")
    add(configuration, "createDependencyReducedPom", "false")
    add(configuration, "outputFile", target)
    configuration.append(copy.deepcopy(relocations))
    transformer = add(add(configuration, "transformers"), "transformer")
    transformer.set("implementation", transformer_name)
    return ET.ElementTree(project), jar_version, shade_version


def shade_test_support(repo, effective_pom, classes, target, work):
    require(classes.is_dir() and any(classes.rglob("*.class")), "empty test class output")
    require(not target.exists() and not work.exists(), "test shading output already exists")
    pom, jar_version, shade_version = test_support_pom(effective_pom, classes, target, work)
    work.mkdir(parents=True)
    pom_path = work / "pom.xml"
    pom.write(pom_path, encoding="utf-8", xml_declaration=True)
    command = [str(repo / "mvnw"), "-o", "-B", "-ntp", "-f", str(pom_path),
               "org.apache.maven.plugins:maven-jar-plugin:" + jar_version + ":jar",
               "org.apache.maven.plugins:maven-shade-plugin:" + shade_version + ":shade"]
    with (work / "shade.log").open("w") as log:
        subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT, check=True)
    require(target.is_file(), "relocated test support missing")
    return {"effective_pom_sha256": digest(effective_pom), "support_pom_sha256": digest(pom_path),
            "jar_plugin_version": jar_version, "shade_plugin_version": shade_version,
            "output_sha256": digest(target)}


def prepare(args):
    repo, out = args.repo.resolve(), args.release.resolve()
    release_validator(repo, out, args.commit, args.lock_sha256)
    require(subprocess.check_output(["git", "-c", "safe.directory=" + str(repo), "rev-parse", "HEAD"], cwd=repo,
                                    text=True).strip() == args.commit, "test-support HEAD mismatch")
    support = out / "test-support"
    require(not support.exists(), "test support already exists; choose a fresh release directory")
    support.mkdir()
    (support / "lib").mkdir()
    (support / "probe").mkdir()
    jar_hashes = {name: digest(out / name) for name in [CORE, DELTA]}
    shading = {}
    for label, module in [("core", "spark"), ("delta", "contrib/delta-spark")]:
        shading[label] = shade_test_support(
            repo, out / "effective-pom.xml", repo / module / "target/test-classes",
            support / (label + "-tests.jar"),
            repo / "target/rc-smoke-support" / out.name / label)
    classpath = ["core-tests.jar", "delta-tests.jar"]
    copied = set()
    for module in ["spark", "contrib/delta-spark"]:
        entries = (repo / module / "target/rc-smoke-classpath.txt").read_text().strip().split(os.pathsep)
        for entry in entries:
            path = Path(entry).resolve()
            # The final shaded core JAR supplies project production classes. The
            # matching test JARs above are the only project test outputs admitted.
            if beneath(path, repo) or path.name.startswith(("comet-", "original-comet-")):
                continue
            require(path.is_file() and path.suffix == ".jar", "non-JAR test dependency: " + entry)
            name = "lib/" + digest(path) + "-" + path.name
            if name not in copied:
                shutil.copyfile(path, support / name)
                copied.add(name)
                classpath.append(name)
    require(copied, "no external test dependencies bundled")
    source = support / "probe/RcPackagedSmoke.java"
    source.write_text(PROBE)
    subprocess.run([str(Path(os.environ["JAVA_HOME"]) / "bin/javac"), "--release", "17",
                    "-d", str(support / "probe"), str(source)], check=True)
    classpath.append("probe")
    final_classes = production_classes(out)
    for name in classpath:
        if name.endswith(".jar"):
            check_test_jar(support / name, final_classes)
    require(jar_hashes == {name: digest(out / name) for name in [CORE, DELTA]},
            "production JAR bytes changed while preparing test support")
    files = {path.relative_to(support).as_posix(): digest(path)
             for path in sorted(support.rglob("*")) if path.is_file()}
    manifest = {"format": 1, "commit": args.commit, "cargo_lock_sha256": args.lock_sha256,
                "release_jar_sha256": {name: digest(out / name) for name in [CORE, DELTA]},
                "effective_pom_sha256": digest(out / "effective-pom.xml"),
                "test_support_shading": shading,
                "classpath": classpath, "files": files}
    (support / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Prepared portable packaged-smoke support for " + args.commit)


def validate_support(out, commit, lock):
    support = out / "test-support"
    manifest = json.loads((support / "manifest.json").read_text())
    require(manifest["format"] == 1 and manifest["commit"] == commit
            and manifest["cargo_lock_sha256"] == lock, "test support provenance mismatch")
    require(manifest["release_jar_sha256"] == {name: digest(out / name) for name in [CORE, DELTA]},
            "test support belongs to different release JARs")
    require(manifest["effective_pom_sha256"] == digest(out / "effective-pom.xml"),
            "test-support POM mismatch")
    for name, sha in manifest["files"].items():
        require(digest(safe_file(support, name)) == sha, "test support checksum mismatch: " + name)
    actual = {p.relative_to(support).as_posix() for p in support.rglob("*")
              if p.is_file() and p.name != "manifest.json"}
    require(actual == set(manifest["files"]), "unexpected or missing test-support files")
    require(manifest["classpath"][:2] == ["core-tests.jar", "delta-tests.jar"]
            and manifest["classpath"][-1] == "probe", "unexpected test classpath")
    classpath = [str(out / CORE), str(out / DELTA)]
    final_classes = production_classes(out)
    for name in manifest["classpath"]:
        if name == "probe":
            require((support / "probe/RcPackagedSmoke.class").is_file(), "missing Java smoke probe")
            probe_files = {p.relative_to(support / "probe").as_posix()
                           for p in (support / "probe").rglob("*") if p.is_file()}
            require(probe_files <= {"RcPackagedSmoke.class", "RcPackagedSmoke.java"},
                    "unexpected class or file in Java smoke probe directory")
            classpath.append(str(support / "probe"))
        else:
            path = safe_file(support, name)
            check_test_jar(path, final_classes)
            classpath.append(str(path))
    return manifest, classpath


def assert_test_summary(text, expected):
    summaries = re.findall(
        r"Tests: succeeded (\d+), failed (\d+), canceled (\d+), ignored (\d+), pending (\d+)", text)
    require(len(summaries) == 1 and tuple(map(int, summaries[0])) == (expected, 0, 0, 0, 0),
            "packaged smoke failed, skipped tests, or ran the wrong number of tests")
    require(re.search(r"Suites: completed \d+, aborted 0", text) is not None,
            "packaged smoke suite aborted or did not report")


def validate_class_origins(loaded, jar_hashes, group):
    expected = {name: {"jar_name": CORE, "jar_sha256": jar_hashes[CORE], "resource_count": 1}
                for name in CORE_ORIGIN_CLASSES}
    if group in DELTA_GROUPS:
        expected.update({name: {"jar_name": DELTA, "jar_sha256": jar_hashes[DELTA],
                               "resource_count": 1} for name in DELTA_ORIGIN_CLASSES})
    origins = loaded.get("class_origins")
    require(isinstance(origins, dict) and set(origins) == set(expected),
            "missing or unexpected final-JAR class origins")
    for name, proof in origins.items():
        require(isinstance(proof, dict) and type(proof.get("resource_count")) is int
                and proof == expected[name], "class origin proof mismatch: " + name)


def run(args):
    repo, out = args.repo.resolve(), args.release.resolve()
    machine = {"amd64": {"x86_64", "amd64"}, "aarch64": {"aarch64", "arm64"}}[args.architecture]
    require(platform.system() == "Linux" and platform.machine().lower() in machine,
            "packaged smoke must execute on the requested native Linux architecture")
    release_validator(repo, out, args.commit, args.lock_sha256)
    manifest, classpath = validate_support(out, args.commit, args.lock_sha256)
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    pom = ET.parse(out / "effective-pom.xml").getroot()
    project = pom if pom.tag.endswith("}project") else pom.find("m:project", ns)
    flags = shlex.split(project.find("m:properties/m:argLine", ns).text)
    require(not any(flag in ["-cp", "-classpath", "--class-path"] for flag in flags),
            "unexpected classpath override in release POM")
    java_home = Path(os.environ["JAVA_HOME"])
    root = out / "smoke" / args.architecture
    require(not root.exists(), "smoke output already exists; preserve the prior result")
    root.mkdir(parents=True)
    (root / "empty-native").mkdir()
    env = dict(os.environ)
    for key in ["JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "CLASSPATH", "LD_PRELOAD"]:
        env.pop(key, None)
    env.update({"SPARK_LOCAL_IP": "127.0.0.1", "SPARK_LOCAL_HOSTNAME": "localhost",
                "COMET_CONF_DIR": str(repo / "conf"), "SPARK_HOME": str(repo),
                "RUST_BACKTRACE": "1", "LD_LIBRARY_PATH": str(java_home / "lib/server")})
    native_sha = digest(out / "linux" / args.architecture / "libcomet.so")
    results = []
    for label, suite, selectors, expected in CASES:
        case_dir = root / label
        case_dir.mkdir()
        tmp = case_dir / "tmp"
        tmp.mkdir()
        proof = case_dir / "loaded-native.json"
        case_flags = flags + [
            "-Djava.awt.headless=true", "-Djava.io.tmpdir=" + str(tmp),
            "-Djava.library.path=" + str(root / "empty-native"),
            "-Dlog4j.configurationFile=file:" + str(repo / "spark/src/test/resources/log4j2.properties"),
            "-Dspark.test.home=" + str(repo), "-Dspark.testing=true",
            "-Dspark.sql.warehouse.dir=" + str(case_dir / "warehouse"),
            "-Dspark.hadoop.fs.s3a.committer.magic.enabled=false",
            "-Drc.arch=" + args.architecture, "-Drc.core=" + str(out / CORE),
            "-Drc.delta=" + str(out / DELTA), "-Drc.native.sha256=" + native_sha,
            "-Drc.proof=" + str(proof), "-Drc.require.delta=" + str(label in DELTA_GROUPS).lower(),
            "-Xlog:class+load=info:file=" + str(case_dir / "classes.log"),
        ]
        command = [str(java_home / "bin/java")] + case_flags + [
            "-cp", os.pathsep.join(classpath), "RcPackagedSmoke", "-oD", "-s", suite] + selectors
        log = case_dir / "tests.log"
        with log.open("w") as stream:
            try:
                proc = subprocess.run(command, env=env, cwd=repo, stdout=stream,
                                      stderr=subprocess.STDOUT, timeout=args.timeout_seconds)
            except subprocess.TimeoutExpired as error:
                raise ValueError(label + " packaged smoke exceeded its timeout") from error
        text = log.read_text(errors="replace")
        require(proc.returncode == 0, label + " packaged smoke JVM failed; inspect " + str(log))
        assert_test_summary(text, expected)
        loaded = json.loads(proof.read_text())
        require(loaded["architecture"] == args.architecture
                and loaded["core_jar_sha256"] == manifest["release_jar_sha256"][CORE]
                and loaded["loaded_native_sha256"] == native_sha, "loaded library proof mismatch")
        validate_class_origins(loaded, manifest["release_jar_sha256"], label)
        results.append({"group": label, "passed": expected, "failed": 0, "canceled": 0,
                        "ignored": 0, "pending": 0,
                        "bundled_native_and_class_origins": "VERIFIED",
                        "loaded_native_proof": loaded})
        print(args.architecture, label, expected, "packaged tests passed", flush=True)
    # Checking again makes the result identify the exact bytes before AND after execution.
    release_validator(repo, out, args.commit, args.lock_sha256)
    validate_support(out, args.commit, args.lock_sha256)
    result = {"commit": args.commit, "architecture": args.architecture,
              "cargo_lock_sha256": args.lock_sha256,
              "release_jar_sha256": manifest["release_jar_sha256"], "native_sha256": native_sha,
              "checksum_manifest_sha256": digest(out / "sha256.txt"),
              "test_support_manifest_sha256": digest(out / "test-support/manifest.json"),
              "cases": results, "total_passed": sum(case["passed"] for case in results)}
    (root / "results.json").write_text(json.dumps(result, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ["prepare", "run"]:
        sub = subparsers.add_parser(name)
        sub.add_argument("--repo", type=Path, required=True)
        sub.add_argument("--release", type=Path, required=True)
        sub.add_argument("--commit", required=True)
        sub.add_argument("--lock-sha256", required=True)
        if name == "run":
            sub.add_argument("--architecture", choices=["amd64", "aarch64"], required=True)
            sub.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            prepare(args)
        else:
            require(args.timeout_seconds > 0, "timeout must be positive")
            run(args)
    except (ValueError, OSError, KeyError, AttributeError, ET.ParseError,
            zipfile.BadZipFile, subprocess.CalledProcessError) as error:
        parser.exit(1, "Packaged runtime verification failed: " + str(error) + "\n")


if __name__ == "__main__":
    main()
