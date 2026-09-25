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
import cats.implicits._
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsEffect
import fs2.io.file.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SnowplowSparkBlockProbe, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructField, StructType, TimestampType}
import org.specs2.Specification

import java.net.URI
import java.time.{Instant, LocalDate}
import java.time.temporal.ChronoUnit

import com.snowplowanalytics.snowplow.lakes.{Config, TestConfig}
import com.snowplowanalytics.snowplow.lakes.fs.LakeLoaderFileSystem
import com.snowplowanalytics.snowplow.lakes.tables.DeltaWriter

import scala.concurrent.duration.DurationInt

// SparkUtils is private[processing], so this test must live in the same package.
class SparkUtilsSpec extends Specification with CatsEffect {
  import SparkUtilsSpec._

  override val Timeout = 60.seconds

  def is = sequential ^ s2"""
  SparkUtils.encodeBatch, stageBatch and appendStagedBatch should:
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
    Accumulate one partition per batch, so the window is not collapsed to a single partition $e13
    Round-trip the java.time values emitted by SparkCaster, which need a lenient encoder $e14
    Encode every row of a batch distinctly, not repeat the last one $e15
    Stage the batch in off-heap memory, not on the JVM heap, when asked to $e16
  SparkUtils.dropView should:
    Release the window's checkpoint blocks, at either staging level, instead of leaving them to the ContextCleaner $e18
  SparkUtils.session built from TestConfig should:
    Reach the staging decision the whole-loader specs expect, on both paths $e17
  SparkUtils.session for a Delta target on GCS should:
    Resolve fs.gs.impl to LakeLoaderFileSystem with the hadoop-gcp connector as delegate $e12
  """

  // Spark's unionByName generates an internal struct-cast target type where every field defaults to
  // nullable=true.  When two consecutive appends use different inner struct schemas (e.g. a
  // patch-version adds a new field), the shared fields that were required in the first schema
  // become nullable in the accumulated view.
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v1"))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v2", null))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row(Row("v2"), null))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row(Seq(Row("v1"))))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row(Seq(Row("v2", null))))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v1"))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v2", null))), schema2, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v3", null))), schema3, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row(null))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v2"))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row("v1")))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row(null)))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Seq(Row("v2", null)))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v1"))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row("v2", null))), schema2, shouldRestoreNullability = true)
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
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row(Row(null)))), schema1, shouldRestoreNullability = true)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(Row(Row("v2", null)))), schema2, shouldRestoreNullability = true)
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

  // The window must keep one partition per batch, not collapse to a single partition (which would
  // serialize the map-side read). Asserts N batches -> N partitions.
  def e13 = withSpark.use { spark =>
    val schema = StructType(Array(StructField("col_a", StringType, nullable = false)))

    // One staging level is enough: both go through the same `checkpointedDataFrame`, and a storage
    // level cannot change how many partitions the staged RDD has.
    val viewName = "test_partition_accumulation_e13"

    def append(row: Row) =
      localAppendRows(spark, viewName, NonEmptyList.one(row), schema, shouldRestoreNullability = true, stageOffHeap = false)

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- append(Row("v1"))
      _ <- append(Row("v2"))
      _ <- append(Row("v3"))
      numPartitions <- IO.blocking(spark.table(viewName).rdd.getNumPartitions)
    } yield numPartitions must beEqualTo(3)
  }

  // SparkCaster emits java.time.Instant for timestamps and java.time.LocalDate for dates. With
  // spark.sql.datetime.java8API.enabled left at its default of false, only a lenient encoder
  // accepts those types - a strict one takes java.sql.Timestamp/Date and fails on these. This
  // pins that, since the failure would otherwise only show up under load with real events.
  def e14 = withSpark.use { spark =>
    val viewName = "test_java8_time_encoding_e14"
    val schema = StructType(
      Array(
        StructField("col_tstamp", TimestampType, nullable = false),
        StructField("col_date", DateType, nullable        = false)
      )
    )
    val instant = Instant.parse("2026-08-27T14:16:00Z")
    val date    = LocalDate.of(2026, 8, 27)

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row(instant, date)), schema, shouldRestoreNullability = true)
      // Read back as epoch micros/days rather than java.sql.Timestamp/Date. The loader never makes
      // that conversion either - it writes parquet straight from the internal representation - and
      // Spark's toJavaDate needs --add-opens java.base/sun.util.calendar on a modern JDK.
      collected <- IO.blocking {
                     spark
                       .table(viewName)
                       .selectExpr("unix_micros(col_tstamp) as micros", "datediff(col_date, to_date('1970-01-01')) as days")
                       .collect()
                       .toList
                       .map(r => (r.getLong(0), r.getInt(1).toLong))
                   }
    } yield collected must beEqualTo(List((ChronoUnit.MICROS.between(Instant.EPOCH, instant), date.toEpochDay)))
  }

  // The encoder returned by rowEncoder reuses a single output row, so encodeBatch must copy each
  // result. Without the copy every row in the batch would come back holding the last row's values.
  def e15 = withSpark.use { spark =>
    val viewName = "test_batch_encoding_distinct_e15"
    val schema   = StructType(Array(StructField("col_a", StringType, nullable = false)))
    val rows     = NonEmptyList.of(Row("v1"), Row("v2"), Row("v3"))

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- localAppendRows(spark, viewName, rows, schema, shouldRestoreNullability = true)
      collected <- IO.blocking(spark.table(viewName).collect().toList.map(_.getString(0)).sorted)
    } yield collected must beEqualTo(List("v1", "v2", "v3"))
  }

  // Spark silently drops useOffHeap when normalising the checkpoint storage level, so assert where
  // the staged bytes landed rather than which level we asked for. SnowplowInternalSparkBridgeSpec
  // covers the level itself.
  def e16 = withSpark.use { spark =>
    val viewName = "test_offheap_staging_e16"
    val schema   = StructType(Array(StructField("col_a", StringType, nullable = false)))

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- localAppendRows(spark, viewName, NonEmptyList.one(Row("v1")), schema, shouldRestoreNullability = true, stageOffHeap = true)
      blocks <- IO.blocking(SnowplowSparkBlockProbe.persistedBlocks(spark))
    } yield {
      val offHeap = blocks.filter(_.useOffHeap)
      // memSize > 0 also rules out `offHeap` being empty, which is what the heap path would leave.
      // diskSize is 0 because nothing here competes for the pool; under load an evicted block on
      // disk is expected, not a fault. See SparkUtils.stageBatchesOffHeap.
      (offHeap.map(_.memSize).sum must be_>(0L)) and
        (offHeap.map(_.diskSize).sum must beEqualTo(0L))
    }
  }

  // AbstractSparkSpec's e12/e13 assert only output correctness, which is identical on both staging
  // paths, so a broken HOCON merge or a leaked session would leave them silently exercising the
  // heap. Assert the decision itself, over the whole chain: TestConfig -> SparkConf -> session ->
  // stageBatchesOffHeap. storageFraction comes from reference.conf, so it also pins the merge.
  def e17 = Files[IO].tempDirectory.use { tmpDir =>
    def decisionFor(stageOffHeap: Boolean): IO[(Boolean, String)] = {
      val config = TestConfig.defaults(TestConfig.Delta, tmpDir, stageOffHeap)
      val delta = config.output.good match {
        case d: Config.Delta => d
        case other           => throw new IllegalStateException(s"Expected a Delta target but got $other")
      }
      SparkUtils.session[IO](config.spark, new DeltaWriter(delta), delta).use { spark =>
        SparkUtils
          .stageBatchesOffHeap[IO](spark)
          .map(offHeap => (offHeap, spark.sparkContext.getConf.get("spark.memory.storageFraction")))
      }
    }

    for {
      requested <- decisionFor(stageOffHeap = true)
      default <- decisionFor(stageOffHeap = false)
    } yield (requested must beEqualTo((true, "0"))) and (default must beEqualTo((false, "0")))
  }

  // Dropping the temp view only removes the catalog entry; the checkpoint blocks stay in the block
  // manager until Spark's ContextCleaner observes the RDD collected, which needs a GC. dropView is
  // given the RDDs so it can release them at the point we know the window is dead. Asserts they are
  // registered as persistent while the window is open, and gone once it is dropped.
  //
  // Both staging levels, because off-heap is the one where waiting for the ContextCleaner hurts
  // most: filling the pool provokes no GC of its own.
  def e18 = withSpark.use { spark =>
    val schema = StructType(Array(StructField("col_a", StringType, nullable = false)))

    def releasedOnDrop(viewName: String, stageOffHeap: Boolean) = {
      def append(row: Row) =
        localAppendRows(spark, viewName, NonEmptyList.one(row), schema, shouldRestoreNullability = true, stageOffHeap = stageOffHeap)

      for {
        _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
        rdds <- List(Row("v1"), Row("v2"), Row("v3")).traverse(append)
        persistedWhileOpen <- IO.blocking(rdds.map(_.id).count(spark.sparkContext.getPersistentRDDs.contains))
        _ <- SparkUtils.dropView[IO](spark, viewName, rdds)
        persistedAfterDrop <- IO.blocking(rdds.map(_.id).count(spark.sparkContext.getPersistentRDDs.contains))
      } yield (rdds.size, persistedWhileOpen, persistedAfterDrop)
    }

    for {
      onHeap <- releasedOnDrop("test_unpersist_on_drop_e18_heap", stageOffHeap = false)
      offHeap <- releasedOnDrop("test_unpersist_on_drop_e18_offheap", stageOffHeap = true)
    } yield (onHeap must beEqualTo((3, 3, 0))) and (offHeap must beEqualTo((3, 3, 0)))
  }

  // Guards the LakeLoaderFileSystem override on GCS: the gs scheme resolves to hadoop-gcp via
  // Hadoop's own core-default.xml, so the override must capture that value as the delegate and
  // stay visible in both sparkContext.hadoopConfiguration (which it patches) and every conf
  // derived from it via sessionState.newHadoopConf() — otherwise Delta would resolve the gs
  // scheme to the raw connector and async delete would be silently disabled on GCS.
  def e12 = Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)
    val delta = config.output.good match {
      case d: Config.Delta => d.copy(location = new URI("gs://bucket/events"))
      case other           => throw new IllegalStateException(s"Expected a Delta target but got $other")
    }
    SparkUtils.session[IO](config.spark, new DeltaWriter(delta), delta).use { spark =>
      IO.blocking {
        val contextConf = spark.sparkContext.hadoopConfiguration
        val derivedConf = spark.sessionState.newHadoopConf()
        val hadoopGcp   = "org.apache.hadoop.fs.gs.GoogleHadoopFileSystem"

        (contextConf.get("fs.gs.impl") must beEqualTo(classOf[LakeLoaderFileSystem].getName)) and
          (derivedConf.get("fs.gs.impl") must beEqualTo(classOf[LakeLoaderFileSystem].getName)) and
          (contextConf.get("fs.gs.lakeloader.delegate.impl") must beEqualTo(hadoopGcp)) and
          (derivedConf.get("fs.gs.lakeloader.delegate.impl") must beEqualTo(hadoopGcp))
      }
    }
  }
}

object SparkUtilsSpec {

  /**
   * `SparkUtils.encodeBatch`, then `stageBatch`, then `appendStagedBatch`.
   *
   * Production code deliberately keeps the three steps apart, so that encoding and staging run
   * outside the append mutex - see `LakeWriter`. These tests are single-threaded and only care
   * about the combined effect, so they stitch them back together here.
   *
   * Passes through the RDD holding the batch's checkpoint blocks, which `e18` asserts on.
   */
  private def localAppendRows(
    spark: SparkSession,
    viewName: String,
    rows: NonEmptyList[Row],
    igluSchema: StructType,
    shouldRestoreNullability: Boolean,
    stageOffHeap: Boolean = false
  ): IO[RDD[InternalRow]] =
    for {
      encoded <- SparkUtils.encodeBatch[IO](spark, rows, igluSchema)
      staged <- SparkUtils.stageBatch[IO](spark, encoded, igluSchema, stageOffHeap)
      _ <- SparkUtils.appendStagedBatch[IO](spark, viewName, staged.df, igluSchema, shouldRestoreNullability)
    } yield staged.checkpointed

  private def withSpark: Resource[IO, SparkSession] = {
    val build = IO.blocking(
      SparkSession
        .builder()
        .master("local")
        .appName("SparkUtilsSpec")
        // e13, e16 and e18 stage off-heap, which needs a pool.
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", 256L * 1024 * 1024)
        // Match reference.conf rather than Spark's default of 0.5: staged blocks own none of the
        // pool and have to borrow from the execution region. e16 passing is what proves borrowing
        // works, so do not "fix" this by giving storage a guaranteed share.
        .config("spark.memory.storageFraction", "0")
        .getOrCreate()
    )
    Resource.make(build)(s => IO.blocking(s.close()))
  }
}
