#!/usr/bin/env bash
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

# Resolve the root Maven model before a compile/test command. Retry only transient
# transfers or an unavailable lookup of this project's known immutable ASF parent.
# A not-found diagnostic does not establish an HTTP status. Never compile or test:
# the nonrecursive validate lifecycle and the allowed arguments are fixed here.
set -euo pipefail
for argument in "$@"; do
  case "$argument" in
    -P?*|-D?*) ;;
    *) echo "Only attached -P/-D options are accepted; no Maven goals: $argument" >&2
       exit 2 ;;
  esac
done
root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
log=$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/comet-maven-model.XXXXXX")
trap 'rm -f "$log"' EXIT
cd "$root_dir"
transfer_failure='Non-resolvable parent POM.*Could not transfer artifact.*status code: (408|429|500|502|503|504)([^0-9]|$)'
# This exact released root parent is declared in pom.xml. Do not generalize this
# exception to other missing coordinates, repositories, plugins or build phases.
known_root_lookup='Non-resolvable parent POM for org[.]apache[.]datafusion:comet-parent-[^[:space:]:]+:[^[:space:]:]+:.*Could not find artifact org[.]apache:apache:pom:23 in central [(]https://repo[.]maven[.]apache[.]org/maven2[)]'
for attempt in 1 2 3; do
  if ./mvnw -B -N -U -Dstyle.color=never "$@" validate 2>&1 | tee "$log"; then
    exit 0
  else
    statuses=("${PIPESTATUS[@]}")
    status=${statuses[0]}
    if [ "${statuses[1]}" -ne 0 ]; then
      echo "Could not record Maven model output" >&2
      exit "${statuses[1]}"
    fi
  fi
  # A resolved model can execute validation/enforcer plugins. Never retry
  # those failures, even if a later diagnostic mentions a repository error.
  if grep -qE '^\[INFO\] (Building |Reactor Build Order:|--- )' "$log" ||
      ! grep -qE "$transfer_failure|$known_root_lookup" "$log"; then
    exit "$status"
  fi
  if [ "$attempt" -eq 3 ]; then
    echo "Maven parent POM remains unavailable after $attempt model-only attempts" >&2
    exit "$status"
  fi
  # Resolver already honors Retry-After and has its own bounded retry budget.
  # Leave a pause between cold-start attempts instead of immediately hammering
  # the repository. -U above avoids reusing a cached failed transfer.
  delay=$((60 * (1 << (attempt - 1)) + RANDOM % 5))
  echo "Transient parent-POM lookup failed; retrying model resolution in ${delay}s" >&2
  sleep "$delay"
done
