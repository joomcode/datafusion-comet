-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.


-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.batchSize=2
-- Config: spark.comet.exec.transitionRevert.enabled=false
-- ConfigMatrix: spark.sql.codegen.wholeStage=true,false

statement
CREATE TABLE g08_or_order(id int, p boolean, q boolean, s string) USING parquet

statement
INSERT INTO g08_or_order SELECT /*+ COALESCE(1) */ * FROM VALUES
  (0, false, false, 'bad'), (1, true, true, '1'), (2, NULL, false, 'bad'),
  (3, true, NULL, 'bad'), (4, false, true, '2'), (5, false, NULL, NULL),
  (6, true, false, '3'), (7, NULL, true, '4'), (8, NULL, NULL, NULL)
  AS v(id, p, q, s)

-- Full three-valued truth table over column input, across several physical batches.
query
SELECT id, p OR q FROM g08_or_order

-- A mixed guard must skip only the malformed row, independent of batch selectivity.
query
SELECT id, (NOT p) OR CAST(s AS INT) > 0 FROM g08_or_order WHERE id < 2

-- NULL does not determine a projected boolean result: the RHS must still be evaluated.
query expect_error_class(CAST_INVALID_INPUT)
SELECT p OR CAST(s AS INT) > 0 FROM g08_or_order WHERE id = 2

-- A determining guard must preserve the entire row without evaluating a failing RHS.
query
SELECT id, (NOT p) OR (CAST(s AS INT) DIV 0 > 0) FROM g08_or_order WHERE id = 0

-- The RHS error is mandatory when the guard does not determine the result.
query expect_error_class(CAST_INVALID_INPUT)
SELECT p OR CAST(s AS INT) > 0 FROM g08_or_order WHERE id = 0
