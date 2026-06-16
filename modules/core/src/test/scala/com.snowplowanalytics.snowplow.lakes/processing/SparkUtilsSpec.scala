/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsEffect
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.specs2.Specification

import scala.concurrent.duration.DurationInt

// SparkUtils is private[processing], so this test must live in the same package.
class SparkUtilsSpec extends Specification with CatsEffect {
  import SparkUtilsSpec._

  override val Timeout = 60.seconds

  def is = sequential ^ s2"""
  SparkUtils.localAppendRows should:
    Preserve required struct field nullability when a second batch introduces a new sub-field $e1
    Preserve required array-element struct field nullability (required elements) when a second batch introduces a new sub-field $e2
    Preserve required array-element struct field nullability (optional elements) when a second batch introduces a new sub-field $e3
    Preserve required field nullability inside a struct-within-struct when a peer sub-field is added at the outer level $e4
    Preserve required field nullability inside an array-within-struct when a new sub-field is added to the element schema $e5
    Preserve nested fields accumulated from earlier batches when a later batch does not include them $e6
    Keep a nested field nullable when the accumulated view already contains nulls for it, even if the current batch's Iglu schema marks it as required $e7
    Preserve array containsNull=true when a later batch introduces a new element sub-field with containsNull=false in the Iglu schema $e8
    Keep array element-struct field nullable when the accumulated array already contains nulls for it $e9
    Keep a newly introduced optional struct field nullable when it has no counterpart in the accumulated view $e10
    Keep a doubly-nested struct field nullable when the accumulated view already contains nulls for it $e11
  """

  // Spark's unionByName generates an internal struct-cast target type where every field defaults to
  // nullable=true.  When two consecutive calls to localAppendRows use different inner struct
  // schemas (e.g. a patch-version adds a new field), the shared fields that were required in the
  // first schema become nullable in the accumulated view.
  //
  // This test reproduces the minimal scenario:
  //   call 1 – schema with  struct<col_a: NOT NULL>
  //   call 2 – schema with  struct<col_a: NOT NULL, col_b: NULL>   (new field added)
  // and asserts that col_a is still NOT NULL in the accumulated view after call 2.
  def e1 = withSpark.use { spark =>
    val viewName = "test_nullability_view"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField("col_a", StringType, nullable = false)
            )
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField("col_a", StringType, nullable = false),
              StructField("col_b", StringType, nullable = true)
            )
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v1"))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v2", null))), schema2, shouldRestoreNullability = true)
      (colANullable, colAValues) <- IO.blocking {
                                      import spark.implicits._
                                      val df = spark.table(viewName)
                                      val colANullable = df.schema.fields
                                        .find(_.name == "s")
                                        .flatMap(_.dataType match {
                                          case st: StructType => st.fields.find(_.name == "col_a").map(_.nullable)
                                          case _              => None
                                        })
                                      val colAValues = df.select("s.col_a").as[String].collect().toSet
                                      (colANullable, colAValues)
                                    }
    } yield (colANullable must beSome(false)) and (colAValues must_== Set("v1", "v2"))
  }

  // context_* columns are ArrayType(StructType(...)) at the top level.  With containsNull=false the
  // lambda variable in transformArray has nullable=false, so GetStructField(x, 0).nullable =
  // false || false = false — col_a is never corrupted and the fix is a no-op.  The test is included
  // to document that the required-elements case is handled correctly (col_a stays NOT NULL).
  def e2 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e2"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(Array(StructField("col_a", StringType, nullable = false))),
            containsNull = false
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(
              Array(
                StructField("col_a", StringType, nullable = false),
                StructField("col_b", StringType, nullable = true)
              )
            ),
            containsNull = false
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils
             .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
      colANullable <- IO.blocking {
                        spark
                          .table(viewName)
                          .schema
                          .fields
                          .find(_.name == "s")
                          .flatMap(_.dataType match {
                            case ArrayType(st: StructType, _) => st.fields.find(_.name == "col_a").map(_.nullable)
                            case _                            => None
                          })
                      }
    } yield colANullable must beSome(false)
  }

  // context_* columns are ArrayType(StructType(...)) at the top level.  When a patch version adds a
  // new sub-field to the element struct, Spark's unionByName widens the element type and makes all
  // element-struct fields nullable.  containsNull=true is required so that the lambda variable in
  // transformArray is nullable, causing GetStructField to propagate nullable to col_a and actually
  // corrupt col_a.nullable from false to true.
  def e3 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e3"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(Array(StructField("col_a", StringType, nullable = false))),
            containsNull = true
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(
              Array(
                StructField("col_a", StringType, nullable = false),
                StructField("col_b", StringType, nullable = true)
              )
            ),
            containsNull = true
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils
             .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
      colANullable <- IO.blocking {
                        spark
                          .table(viewName)
                          .schema
                          .fields
                          .find(_.name == "s")
                          .flatMap(_.dataType match {
                            case ArrayType(st: StructType, _) => st.fields.find(_.name == "col_a").map(_.nullable)
                            case _                            => None
                          })
                      }
    } yield colANullable must beSome(false)
  }

  // struct-within-struct: the top-level column is a struct whose inner field is itself a struct.
  // A patch version adds a new optional sub-field alongside the inner struct at the outer struct
  // level.  unionByName extracts `outer` via GetStructField(s, 0), which inherits s's nullable and
  // corrupts outer.nullable from false to true.
  def e4 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e4"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField(
                "outer",
                StructType(
                  Array(
                    StructField("col_a", StringType, nullable = false)
                  )
                ),
                nullable = false
              )
            )
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField(
                "outer",
                StructType(
                  Array(
                    StructField("col_a", StringType, nullable = false)
                  )
                ),
                nullable = false
              ),
              StructField("new_outer_field", StringType, nullable = true)
            )
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils
             .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row(Row("v2"), null))), schema2, shouldRestoreNullability = true)
      outerNullable <- IO.blocking {
                         spark
                           .table(viewName)
                           .schema
                           .fields
                           .find(_.name == "s")
                           .flatMap(_.dataType match {
                             case st: StructType => st.fields.find(_.name == "outer").map(_.nullable)
                             case _              => None
                           })
                       }
    } yield outerNullable must beSome(false)
  }

  // array-within-struct: the top-level column is a struct containing an array of structs.  A patch
  // version adds a new optional sub-field to the element schema.  containsNull=true is required so
  // that the lambda variable in transformArray is nullable, causing GetStructField to propagate
  // nullable to col_a and actually corrupt col_a.nullable from false to true.
  def e5 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e5"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField(
                "arr",
                ArrayType(
                  StructType(Array(StructField("col_a", StringType, nullable = false))),
                  containsNull = true
                ),
                nullable = false
              )
            )
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField(
                "arr",
                ArrayType(
                  StructType(
                    Array(
                      StructField("col_a", StringType, nullable = false),
                      StructField("col_b", StringType, nullable = true)
                    )
                  ),
                  containsNull = true
                ),
                nullable = false
              )
            )
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils
             .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row(Seq(Row("v1"))))), schema1, shouldRestoreNullability = true)
      _ <-
        SparkUtils
          .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row(Seq(Row("v2", null))))), schema2, shouldRestoreNullability = true)
      colANullable <- IO.blocking {
                        spark
                          .table(viewName)
                          .schema
                          .fields
                          .find(_.name == "s")
                          .flatMap(_.dataType match {
                            case st: StructType =>
                              st.fields
                                .find(_.name == "arr")
                                .flatMap(_.dataType match {
                                  case ArrayType(elemType: StructType, _) =>
                                    elemType.fields.find(_.name == "col_a").map(_.nullable)
                                  case _ => None
                                })
                            case _ => None
                          })
                      }
    } yield colANullable must beSome(false)
  }

  // Three batches where batch3 introduces a sub-field (col_c) that batch2 did not have, while batch2
  // introduced a sub-field (col_b) that batch3 does not have.  After all three batches the union
  // result contains col_a, col_b, and col_c.  correctedSchema must iterate over the union result's
  // nested fields (not only the current batch's) so that col_b is preserved in the view.
  def e6 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e6"

    val schema1 = StructType(
      Array(
        StructField("s", StructType(Array(StructField("col_a", StringType, nullable = false))), nullable = true)
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          StructType(Array(StructField("col_a", StringType, nullable = false), StructField("col_b", StringType, nullable = true))),
          nullable = true
        )
      )
    )

    val schema3 = StructType(
      Array(
        StructField(
          "s",
          StructType(Array(StructField("col_a", StringType, nullable = false), StructField("col_c", StringType, nullable = true))),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v1"))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v2", null))), schema2, shouldRestoreNullability = true)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v3", null))), schema3, shouldRestoreNullability = true)
      (colANullable, colBNullable) <- IO.blocking {
                                        val inner = spark
                                          .table(viewName)
                                          .schema
                                          .fields
                                          .find(_.name == "s")
                                          .flatMap(_.dataType match {
                                            case st: StructType => Some(st)
                                            case _              => None
                                          })
                                        val colANullable = inner.flatMap(_.fields.find(_.name == "col_a").map(_.nullable))
                                        val colBNullable = inner.flatMap(_.fields.find(_.name == "col_b").map(_.nullable))
                                        (colANullable, colBNullable)
                                      }
    } yield (colANullable must beSome(false)) and (colBNullable must beSome(true))
  }

  // Regression test for the case where an earlier batch processed a more permissive Iglu schema
  // (col_a nullable=true, and actual nulls were written), and a later batch arrives with a less
  // permissive schema (col_a nullable=false).  restoreNullability must not mark col_a as NOT NULL
  // in the accumulated view, because the view already contains real nulls for that field.
  //
  //   call 1 – schema with  struct<col_a: NULL>     → inserts a row with col_a = null
  //   call 2 – schema with  struct<col_a: NOT NULL> → inserts a row with col_a = "v2"
  //
  // After both calls col_a must remain nullable=true, and the null from call 1 must still be present.
  def e7 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e7"

    val schema1 = StructType(
      Array(
        StructField("s", StructType(Array(StructField("col_a", StringType, nullable = true))), nullable = true)
      )
    )

    val schema2 = StructType(
      Array(
        StructField("s", StructType(Array(StructField("col_a", StringType, nullable = false))), nullable = true)
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row(null))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v2"))), schema2, shouldRestoreNullability = true)
      (colANullable, nullCount) <- IO.blocking {
                                     import spark.implicits._
                                     val df = spark.table(viewName)
                                     val colANullable = df.schema.fields
                                       .find(_.name == "s")
                                       .flatMap(_.dataType match {
                                         case st: StructType => st.fields.find(_.name == "col_a").map(_.nullable)
                                         case _              => None
                                       })
                                     val nullCount = df.select("s.col_a").as[String].collect().count(_ == null)
                                     (colANullable, nullCount)
                                   }
    } yield (colANullable must beSome(true)) and (nullCount must_== 1)
  }

  // When the accumulated view has containsNull=true (a prior batch permitted null array elements),
  // restoreNullability must not flip containsNull to false just because the current batch's Iglu
  // schema marks the array as containsNull=false.
  def e8 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e8"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(Array(StructField("col_a", StringType, nullable = false))),
            containsNull = true
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(
              Array(
                StructField("col_a", StringType, nullable = false),
                StructField("col_b", StringType, nullable = true)
              )
            ),
            containsNull = false
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils
             .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
      containsNull <- IO.blocking {
                        spark
                          .table(viewName)
                          .schema
                          .fields
                          .find(_.name == "s")
                          .flatMap(_.dataType match {
                            case ArrayType(_, cn) => Some(cn)
                            case _                => None
                          })
                      }
    } yield containsNull must beSome(true)
  }

  // Like e7 but the field with historical nulls lives inside an array element struct.  When the
  // accumulated element struct has col_a nullable=true and the current Iglu schema marks it NOT NULL,
  // col_a must remain nullable inside the array.
  def e9 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e9"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(Array(StructField("col_a", StringType, nullable = true))),
            containsNull = true
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          ArrayType(
            StructType(
              Array(
                StructField("col_a", StringType, nullable = false),
                StructField("col_b", StringType, nullable = true)
              )
            ),
            containsNull = true
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row(null)))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils
             .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
      colANullable <- IO.blocking {
                        spark
                          .table(viewName)
                          .schema
                          .fields
                          .find(_.name == "s")
                          .flatMap(_.dataType match {
                            case ArrayType(st: StructType, _) => st.fields.find(_.name == "col_a").map(_.nullable)
                            case _                            => None
                          })
                      }
    } yield colANullable must beSome(true)
  }

  // A newly introduced optional field (nullable=true in the current Iglu schema) that has no
  // counterpart in the accumulated view must remain nullable=true after restoreNullability.
  // The absence of an accumulated entry must not clamp the field to NOT NULL.
  def e10 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e10"

    val schema1 = StructType(
      Array(
        StructField("s", StructType(Array(StructField("col_a", StringType, nullable = false))), nullable = true)
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          StructType(Array(StructField("col_a", StringType, nullable = false), StructField("col_b", StringType, nullable = true))),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v1"))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row("v2", null))), schema2, shouldRestoreNullability = true)
      colBNullable <- IO.blocking {
                        spark
                          .table(viewName)
                          .schema
                          .fields
                          .find(_.name == "s")
                          .flatMap(_.dataType match {
                            case st: StructType => st.fields.find(_.name == "col_b").map(_.nullable)
                            case _              => None
                          })
                      }
    } yield colBNullable must beSome(true)
  }

  // Like e7 but col_a is nested two struct levels deep (s.outer.col_a).  When col_a was nullable
  // in an earlier batch and the later Iglu schema marks it NOT NULL, the accumulated nullability
  // at the inner struct level must be consulted and col_a must remain nullable.
  def e11 = withSpark.use { spark =>
    val viewName = "test_nullability_view_e11"

    val schema1 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField(
                "outer",
                StructType(Array(StructField("col_a", StringType, nullable = true))),
                nullable = false
              )
            )
          ),
          nullable = true
        )
      )
    )

    val schema2 = StructType(
      Array(
        StructField(
          "s",
          StructType(
            Array(
              StructField(
                "outer",
                StructType(
                  Array(
                    StructField("col_a", StringType, nullable = false),
                    StructField("col_b", StringType, nullable = true)
                  )
                ),
                nullable = false
              )
            )
          ),
          nullable = true
        )
      )
    )

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- SparkUtils.localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row(Row(null)))), schema1, shouldRestoreNullability = true)
      _ <- SparkUtils
             .localAppendRows[IO](spark, viewName, NonEmptyList.one(Row(Row(Row("v2", null)))), schema2, shouldRestoreNullability = true)
      colANullable <- IO.blocking {
                        spark
                          .table(viewName)
                          .schema
                          .fields
                          .find(_.name == "s")
                          .flatMap(_.dataType match {
                            case outerSt: StructType =>
                              outerSt.fields
                                .find(_.name == "outer")
                                .flatMap(_.dataType match {
                                  case innerSt: StructType => innerSt.fields.find(_.name == "col_a").map(_.nullable)
                                  case _                   => None
                                })
                            case _ => None
                          })
                      }
    } yield colANullable must beSome(true)
  }
}

object SparkUtilsSpec {
  private def withSpark: Resource[IO, SparkSession] = {
    val build = IO.blocking(
      SparkSession.builder().master("local").appName("SparkUtilsSpec").getOrCreate()
    )
    Resource.make(build)(s => IO.blocking(s.close()))
  }
}
