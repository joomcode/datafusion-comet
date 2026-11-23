#!/usr/bin/env bash
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

set -euo pipefail
cd "${GITHUB_WORKSPACE:?Run this setup inside the Spark SQL CI container}"
apt-get update
apt-get install -y --no-install-recommends python3-venv
python3 -m venv target/spark42-python
target/spark42-python/bin/python -m pip install --only-binary=:all: --require-hashes \
  -r dev/ci/requirements-spark42-python.txt
export PYSPARK_PYTHON="$GITHUB_WORKSPACE/target/spark42-python/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
printf 'PYSPARK_PYTHON=%s\nPYSPARK_DRIVER_PYTHON=%s\n' \
  "$PYSPARK_PYTHON" "$PYSPARK_DRIVER_PYTHON" >> "$GITHUB_ENV"
# Use Spark's actual source checkout and its own prerequisite checks. Installing
# another pyspark distribution could conceal a mismatch with the tested JVM.
"$PYSPARK_PYTHON" -B dev/ci/verify-spark42-python.py apache-spark \
  --output target/spark42-python/prerequisites.json
