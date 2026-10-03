/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.rules

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType, UserDefinedType}

object LeafColumns {

  def count(dataType: DataType): Int = dataType match {
    case struct: StructType => struct.fields.map(f => count(f.dataType)).sum
    case array: ArrayType => count(array.elementType)
    case map: MapType => count(map.keyType) + count(map.valueType)
    case udt: UserDefinedType[_] => count(udt.sqlType)
    case _ => 1
  }

  def count(attributes: Seq[Attribute]): Int = attributes.map(a => count(a.dataType)).sum

  def isNested(dataType: DataType): Boolean = dataType match {
    case _: StructType | _: ArrayType | _: MapType => true
    case udt: UserDefinedType[_] => isNested(udt.sqlType)
    case _ => false
  }

  /** The attributes that none of `keys` references. */
  def outside(attributes: Seq[Attribute], keys: Seq[Expression]): Seq[Attribute] = {
    val referenced = AttributeSet(keys.flatMap(_.references))
    attributes.filterNot(referenced.contains)
  }
}
