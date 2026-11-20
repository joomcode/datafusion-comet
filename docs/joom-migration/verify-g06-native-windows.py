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

"""Verify G06 Spark 4.2 window qualification against actual JNI native plans.

Run CometWindowSemanticsSuite and capture complete merged stdout/stderr with native
explain enabled, then pass that log and an explicit JSON output path. The expected
action matrix is derived from the suite's inputs/configurations, independently of
the required/forbidden strings printed in the log. Keep it synchronized when
intentionally changing that matrix. This verifier supplements Scala's exact row,
schema, mandatory-error, ordering, distribution and window SQL metric assertions;
it does not infer that a test passed from a native plan alone.
"""

import argparse
import json
from pathlib import Path
import re


PARTITION = "PartitionAggregateWindowExec"
SORTED = "CometSortedWindowExec"
BOUNDED = "BoundedWindowAggExec"
WINDOWS = (PARTITION, SORTED, BOUNDED, "WindowAggExec")
BEGIN = re.compile(r"G06_WINDOW_BEGIN\t([^\t\r\n]+)\t([^\t\r\n]*)\t([^\r\n]*)")
END = re.compile(r"G06_WINDOW_END\t([^\r\n]+)")


def expected_actions():
    actions = {}

    def add(name, required=(), forbidden=(), spill=False):
        if name in actions:
            raise ValueError(f"Duplicate expected action {name}")
        actions[name] = dict(required=required, forbidden=forbidden, spill=spill)

    for enabled in (False, True):
        value = str(enabled).lower()
        required = (PARTITION,) if enabled else ()
        forbidden = () if enabled else WINDOWS
        for batch in (3, 64):
            for unit in ("ROWS", "RANGE"):
                add(f"whole-{value}-{batch}-{unit}", required, forbidden)
        for batch in (1, 3, 64):
            add(f"whole-cancellation-{value}-{batch}", required, forbidden)
    add("whole-empty")

    for batch in (3, 64):
        for order in ("o ASC NULLS FIRST", "o DESC NULLS LAST"):
            add(f"distribution-{batch}-{order}", (PARTITION,))
            add(f"ntile-{batch}-{order}", (PARTITION,))
    for data_type in ("FLOAT", "DOUBLE"):
        for nested in ("false", "true"):
            add(f"suffix-zero-{data_type}-{nested}", forbidden=WINDOWS)

    row_frames = (
        "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
        "ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING",
        "ROWS BETWEEN 3 FOLLOWING AND UNBOUNDED FOLLOWING",
    )
    range_frames = (
        ("RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING", "o ASC NULLS FIRST"),
        ("RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING", "o ASC NULLS LAST"),
        ("RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING", "o DESC NULLS LAST"),
    )
    for frame in row_frames:
        add(f"suffix-o, id-{frame}", (PARTITION,))
        add(f"suffix-values-{frame}", (PARTITION,))
    for frame, order in range_frames:
        add(f"suffix-{order}-{frame}", (PARTITION,))
    add("suffix-floating-cancellation", forbidden=WINDOWS)
    add("suffix-original-oracle", forbidden=WINDOWS)
    for function in ("SUM", "AVG", "TRY_AVG"):
        add(f"suffix-decimal-{function}", forbidden=WINDOWS)

    for sorted_enabled in (False, True):
        value = str(sorted_enabled).lower()
        for batch in (3, 64):
            for order in ("ASC NULLS FIRST", "DESC NULLS LAST"):
                add(f"rank-{value}-{batch}-{order}",
                    (SORTED,) if sorted_enabled else (BOUNDED,),
                    () if sorted_enabled else (SORTED,))
        for batch in (3, 128):
            for offset in (-2, 0, 1, 3, 1024, 1025):
                eligible = sorted_enabled and abs(offset) <= 1024
                add(f"shift-{value}-{batch}-{offset}",
                    (SORTED,) if eligible else (BOUNDED,),
                    () if eligible else (SORTED,))
    add("shift-ignore-nulls", forbidden=WINDOWS)
    add("shift-column-default", forbidden=WINDOWS)
    for frame in (
        "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
    ):
        add(f"filter-{frame}", forbidden=WINDOWS)
    for adaptive in ("false", "true"):
        for mode in ("native", "jvm"):
            for function in ("ROW_NUMBER()", "MAX(v)"):
                required = ((SORTED,) if function == "ROW_NUMBER()" else (PARTITION,))
                add(f"aqe-{adaptive}-{mode}-{function}",
                    required if mode == "native" else (),
                    () if mode == "native" else WINDOWS)
    for shape in ("whole", "size", "suffix"):
        add(f"spill-{shape}", (PARTITION,), spill=True)
    return actions


def metric_lines(body, name):
    # A test title, projection expression, pre-execution plan or generic Spark wrapper
    # cannot count as native execution. The actual operator must publish metrics.
    node = re.compile(r"^\s*" + re.escape(name) + r"(?:\s|:|,|$)")
    return [line for line in body if node.search(line) and "metrics=[" in line]


def positive_metric(line, name):
    value = re.search(r"(?:\[|,\s*)" + re.escape(name) + r"=([0-9]+(?:\.[0-9]+)?)", line)
    return value is not None and float(value.group(1)) > 0


def verify(log):
    expected = expected_actions()
    active = None
    records = []
    errors = []
    seen = set()
    with log.open(errors="replace") as source:
        for number, line in enumerate(source, 1):
            if match := BEGIN.search(line):
                if active is not None:
                    errors.append(f"Missing end for {active['name']} before line {number}")
                name, required, forbidden = match.groups()
                active = dict(name=name, begin_line=number, body=[],
                              marker_required=required, marker_forbidden=forbidden)
            elif match := END.search(line):
                name = match.group(1).strip()
                if active is None or active["name"] != name:
                    errors.append(f"Unmatched action end {name!r} at line {number}")
                    continue
                body = active.pop("body")
                before = len(errors)
                if name in seen:
                    errors.append(f"Duplicate action {name}")
                seen.add(name)
                policy = expected.get(name)
                if policy is None:
                    errors.append(f"Unexpected action {name}; update the source-derived matrix")
                    policy = dict(required=(), forbidden=(), spill=False)
                required = set(policy["required"]) | set(filter(None, active["marker_required"].split(",")))
                forbidden = set(policy["forbidden"]) | set(filter(None, active["marker_forbidden"].split(",")))
                for node in sorted(required):
                    if not any(positive_metric(row, "output_rows") for row in metric_lines(body, node)):
                        errors.append(f"{name}: no executed {node} with positive output_rows")
                for node in sorted(forbidden):
                    if metric_lines(body, node):
                        errors.append(f"{name}: forbidden native window {node} executed")
                if policy["spill"] and not any(
                    positive_metric(row, "spill_count") and positive_metric(row, "spilled_bytes")
                    for row in metric_lines(body, PARTITION)
                ):
                    errors.append(f"{name}: no actual partition-window spill count and bytes")
                active.update(end_line=number, passed=len(errors) == before)
                records.append(active)
                active = None
            elif active is not None:
                active["body"].append(line)
    if active is not None:
        errors.append(f"Unterminated action {active['name']}")
    if missing := sorted(set(expected) - seen):
        errors.append(f"Missing qualification actions: {missing}")
    return dict(log=str(log.resolve()), passed=not errors, expected_actions=len(expected),
                observed_actions=len(records), errors=errors, records=records,
                limitation="Verify the complete Scala test result separately; native evidence "
                           "does not replace exact rows, mandatory errors or JVM assertions.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.log)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in
                      ("passed", "expected_actions", "observed_actions", "errors")}))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
