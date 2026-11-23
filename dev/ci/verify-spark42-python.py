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

"""Fail before Spark test discovery if pandas scenarios would be silently skipped."""
import argparse
import importlib.metadata
import json
import os
import re
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spark", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    spark = args.spark.resolve()
    py4j = list((spark / "python/lib").glob("py4j-*-src.zip"))
    if len(py4j) != 1:
        raise RuntimeError("Expected exactly one Py4J archive from the tested Spark checkout")
    for key in ["PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"]:
        if Path(os.environ[key]).absolute() != Path(sys.executable).absolute():
            raise RuntimeError(key + " does not identify the prerequisite-check interpreter")
    sys.path[:0] = [str(spark / "python"), str(py4j[0])]
    import pyspark
    from pyspark.sql.pandas.utils import (
        require_minimum_pandas_version, require_minimum_pyarrow_version)
    if pyspark.__version__ != "4.2.1":
        raise RuntimeError("Wrong PySpark source version: " + pyspark.__version__)
    if not Path(pyspark.__file__).resolve().is_relative_to(spark / "python"):
        raise RuntimeError("PySpark did not load from the tested source checkout")
    require_minimum_pandas_version()
    require_minimum_pyarrow_version()
    requirements = Path(__file__).with_name("requirements-spark42-python.txt").read_text()
    expected = dict(re.findall(r"^([\w-]+)==([\w.]+) \\?$", requirements, re.MULTILINE))
    if len(expected) != 7:
        raise RuntimeError("Incomplete pinned Python dependency set")
    actual = {name: importlib.metadata.version(name) for name in expected}
    if actual != expected:
        raise RuntimeError("Python package versions differ from the checked wheel lock: " + str(actual))
    proof = {"spark": pyspark.__version__, "python": sys.version,
             "executable": sys.executable, "pyspark_source": pyspark.__file__,
             "packages": actual, "spark_pandas_prerequisites": "PASSED"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(proof, indent=2) + "\n")
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
