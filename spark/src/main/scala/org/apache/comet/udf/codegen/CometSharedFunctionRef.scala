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

package org.apache.comet.udf.codegen

// scalastyle:off argcount
sealed trait CometSharedFunctionRef extends Serializable {
  def id: Long
}

object CometSharedFunctionRef {
  def apply(id: Long, arity: Int): CometSharedFunctionRef = arity match {
    case 0 => CometSharedFunction0(id)
    case 1 => CometSharedFunction1(id)
    case 2 => CometSharedFunction2(id)
    case 3 => CometSharedFunction3(id)
    case 4 => CometSharedFunction4(id)
    case 5 => CometSharedFunction5(id)
    case 6 => CometSharedFunction6(id)
    case 7 => CometSharedFunction7(id)
    case 8 => CometSharedFunction8(id)
    case 9 => CometSharedFunction9(id)
    case 10 => CometSharedFunction10(id)
    case 11 => CometSharedFunction11(id)
    case 12 => CometSharedFunction12(id)
    case 13 => CometSharedFunction13(id)
    case 14 => CometSharedFunction14(id)
    case 15 => CometSharedFunction15(id)
    case 16 => CometSharedFunction16(id)
    case 17 => CometSharedFunction17(id)
    case 18 => CometSharedFunction18(id)
    case 19 => CometSharedFunction19(id)
    case 20 => CometSharedFunction20(id)
    case 21 => CometSharedFunction21(id)
    case 22 => CometSharedFunction22(id)
  }

  def unresolved(id: Long): IllegalStateException =
    new IllegalStateException(s"Comet shared UDF function $id was not resolved")
}

final case class CometSharedFunction0(id: Long) extends (() => Any) with CometSharedFunctionRef {
  override def apply(): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction1(id: Long)
    extends ((Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction2(id: Long)
    extends ((Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any, a2: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction3(id: Long)
    extends ((Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any, a2: Any, a3: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction4(id: Long)
    extends ((Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any, a2: Any, a3: Any, a4: Any): Any =
    throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction5(id: Long)
    extends ((Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any, a2: Any, a3: Any, a4: Any, a5: Any): Any =
    throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction6(id: Long)
    extends ((Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any): Any =
    throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction7(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any): Any =
    throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction8(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any, a8: Any)
      : Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction9(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction10(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction11(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction12(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction13(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction14(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction15(id: Long)
    extends ((Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction16(id: Long)
    extends (
        (
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any,
      a16: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction17(id: Long)
    extends (
        (
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any,
      a16: Any,
      a17: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction18(id: Long)
    extends (
        (
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any,
      a16: Any,
      a17: Any,
      a18: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction19(id: Long)
    extends (
        (
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any,
      a16: Any,
      a17: Any,
      a18: Any,
      a19: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction20(id: Long)
    extends (
        (
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any,
      a16: Any,
      a17: Any,
      a18: Any,
      a19: Any,
      a20: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction21(id: Long)
    extends (
        (
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any,
      a16: Any,
      a17: Any,
      a18: Any,
      a19: Any,
      a20: Any,
      a21: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}

final case class CometSharedFunction22(id: Long)
    extends (
        (
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any) => Any)
    with CometSharedFunctionRef {
  override def apply(
      a1: Any,
      a2: Any,
      a3: Any,
      a4: Any,
      a5: Any,
      a6: Any,
      a7: Any,
      a8: Any,
      a9: Any,
      a10: Any,
      a11: Any,
      a12: Any,
      a13: Any,
      a14: Any,
      a15: Any,
      a16: Any,
      a17: Any,
      a18: Any,
      a19: Any,
      a20: Any,
      a21: Any,
      a22: Any): Any = throw CometSharedFunctionRef.unresolved(id)
}
// scalastyle:on argcount
