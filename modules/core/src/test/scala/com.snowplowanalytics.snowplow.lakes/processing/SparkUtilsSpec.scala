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
import org.apache.spark.sql.{DataFrame, Row, SnowplowSparkBlockProbe, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Literal}
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructField, StructType, TimestampType}
import org.specs2.Specification

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate}
import java.time.temporal.ChronoUnit
import java.util.UUID

import scala.jdk.CollectionConverters._

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
  SparkUtils.prepareFinalDataFrame should:
    Balance the partitions within writerParallelism when one event_name dominates the window $e24
    Keep a small event_name whole, in a single partition, so output files are not fragmented $e25
    Write a slow trickle into a single partition, instead of one file per event $e26
    Balance a window whose events all have a null event_name, with no special case for it $e27
    Fold a split key's partitions into one literal array, rather than build one per row $e28
  SparkUtils.dropView should:
    Release the window's checkpoint blocks, at either staging level, instead of leaving them to the ContextCleaner $e18
    Release the window's shuffle as well, when prepareCommit left one behind $e19
  SparkUtils.materializeShuffle should:
    Leave the staged blocks releasable without breaking the returned DataFrame $e20
    Keep the staged blocks, but still replace the view, when writerParallelism leaves no exchange $e21
  SparkUtils.readFinalDataFrame should:
    Stamp load_tstamp afresh on every read, so a retried commit does not reuse an older value $e22
  SparkUtils.session built from TestConfig should:
    Reach the staging decision the whole-loader specs expect, on both paths $e17
    Give Spark the core count it was asked for, even against a spark.master in spark.conf $e23
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
      SparkUtils.session[IO](config.spark, new DeltaWriter(delta), delta, cores = 2).use { spark =>
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

  // writerParallelism is derived from the same core count this master is built from, so the
  // reserved task slot only exists if the count actually reaches Spark. spark.conf is unvalidated
  // and spark.master is a legitimate key in it, so the loader applies master after that map -
  // otherwise a deployment could halve Spark's slots while writerParallelism kept counting the
  // whole machine, and nothing would say so.
  // Depends on no other spec holding a session: `getOrCreate` returns a live one and ignores the
  // builder's master, which would make this assert about someone else's session. `fork := true` with
  // sbt's default `testForkedParallel` of false is what keeps suites from overlapping, and every
  // session-creating spec here is `sequential`. Unlike e17 and e12, this one asserts a value that
  // differs between sessions, so it is the example that would report the breakage.
  def e23 = Files[IO].tempDirectory.use { tmpDir =>
    val base = TestConfig.defaults(TestConfig.Delta, tmpDir)
    val delta = base.output.good match {
      case d: Config.Delta => d
      case other           => throw new IllegalStateException(s"Expected a Delta target but got $other")
    }
    val sparkConfig = base.spark.copy(conf = base.spark.conf + ("spark.master" -> "local[1]"))

    SparkUtils.session[IO](sparkConfig, new DeltaWriter(delta), delta, cores = 3).use { spark =>
      IO.blocking((spark.sparkContext.master, spark.sparkContext.defaultParallelism))
    } map { case (master, parallelism) =>
      (master.startsWith("local[3,"), parallelism) must beEqualTo((true, 3))
    }
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
        _ <- SparkUtils.dropView[IO](spark, viewName, rdds, None)
        persistedAfterDrop <- IO.blocking(rdds.map(_.id).count(spark.sparkContext.getPersistentRDDs.contains))
      } yield (rdds.size, persistedWhileOpen, persistedAfterDrop)
    }

    for {
      onHeap <- releasedOnDrop("test_unpersist_on_drop_e18_heap", stageOffHeap = false)
      offHeap <- releasedOnDrop("test_unpersist_on_drop_e18_offheap", stageOffHeap = true)
    } yield (onHeap must beEqualTo((3, 3, 0))) and (offHeap must beEqualTo((3, 3, 0)))
  }

  // dropView is the guaranteed finalizer, so it has to finish whatever prepareCommit started -
  // including the shuffle, which by then is the only copy of the window's data.
  def e19 = withSpark.use { spark =>
    val viewName = "test_drop_releases_shuffle_e19"

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      rdd <- localAppendRows(spark, viewName, eventRows("e1", "e2"), eventSchema, shouldRestoreNullability = true)
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 2, defaultPartitioning, pageViewCounts(2))
      shuffled <- SparkUtils.materializeShuffle[IO](spark, df)
      shuffleId = shuffled.shuffle.map(_.id).getOrElse(throw new IllegalStateException("Expected a shuffle id"))
      registeredBefore <- IO.blocking(SnowplowSparkBlockProbe.shuffleRegistered(shuffleId))
      _ <- SparkUtils.dropView[IO](spark, viewName, List(rdd), shuffled.shuffle.map(_.id))
      registeredAfter <- IO.blocking(SnowplowSparkBlockProbe.shuffleRegistered(shuffleId))
    } yield (registeredBefore, registeredAfter) must beEqualTo((true, false))
  }

  // The SparkUtils-level statement of the property SnowplowInternalSparkBridgeSpec e7 pins, over
  // the composition LakeWriter.prepareCommit actually performs: after materializeShuffle and
  // replaceView, the staged blocks are dead and reading the view still works.
  def e20 = withSpark.use { spark =>
    val viewName = "test_materialize_shuffle_e20"

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      rdd <- localAppendRows(spark, viewName, eventRows("e1", "e2", "e3"), eventSchema, shouldRestoreNullability = true)
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 2, defaultPartitioning, pageViewCounts(3))
      shuffled <- SparkUtils.materializeShuffle[IO](spark, df)
      _ <- SparkUtils.replaceView[IO](viewName, shuffled.df)
      _ <- SparkUtils.releaseStagedBatches[IO](List(rdd))
      persisted <- IO.blocking(spark.sparkContext.getPersistentRDDs.contains(rdd.id))
      collected <- IO.blocking(spark.table(viewName).collect().toList.map(_.getAs[String]("event_id")).sorted)
    } yield (persisted, collected) must beEqualTo((false, List("e1", "e2", "e3")))
  }

  // chooseWriterParallelism is availableProcessors - 1, so a single-core CI machine takes the
  // coalesce branch and there is no exchange to hand the window over to. prepareCommit must then
  // keep the staged batches, because the view still reads from them - but it must still replace the
  // view, or the write would go over the accumulated union rather than the coalesced form. The
  // partition count is what separates those two: one batch stages as one partition, so a view still
  // holding the union of two batches has two. Neither half is exercised on a multi-core dev machine.
  def e21 = withSpark.use { spark =>
    val viewName = "test_no_exchange_e21"

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      rdds <- List(eventRows("e1"), eventRows("e2")).traverse(
                localAppendRows(spark, viewName, _, eventSchema, shouldRestoreNullability = true)
              )
      partitionsBefore <- IO.blocking(spark.table(viewName).queryExecution.toRdd.getNumPartitions)
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 1, defaultPartitioning, pageViewCounts(2))
      shuffled <- SparkUtils.materializeShuffle[IO](spark, df)
      _ <- SparkUtils.replaceView[IO](viewName, shuffled.df)
      persisted <- IO.blocking(rdds.forall(rdd => spark.sparkContext.getPersistentRDDs.contains(rdd.id)))
      partitionsAfter <- IO.blocking(spark.table(viewName).queryExecution.toRdd.getNumPartitions)
      collected <- IO.blocking(spark.table(viewName).collect().toList.map(_.getAs[String]("event_id")).sorted)
    } yield (shuffled.shuffle.map(_.id), persisted, partitionsBefore, partitionsAfter, collected) must beEqualTo(
      (Option.empty[Int], true, 2, 1, List("e1", "e2"))
    )
  }

  // load_tstamp is assigned when the view is read back, not before the shuffle, so that a retried
  // commit stamps its rows with the attempt that succeeds. Assigning it earlier would fix the value
  // in the DataFrame prepareCommit registers, and the setup-error retry has no attempt cap, so rows
  // could land arbitrarily long after the timestamp they carry. Also pins that one read yields a
  // single literal, which is what keeps the writers' open-file count down - see `Writer.write`.
  def e22 = withSpark.use { spark =>
    val viewName = "test_load_tstamp_per_read_e22"

    def stampsFromOneAttempt =
      for {
        df <- SparkUtils.readFinalDataFrame[IO](spark, viewName)
        stamps <- IO.blocking(df.collect().toList.map(_.getAs[java.sql.Timestamp]("load_tstamp")).distinct)
      } yield stamps

    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      _ <- localAppendRows(spark, viewName, eventRows("e1", "e2"), eventSchema, shouldRestoreNullability = true)
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 2, defaultPartitioning, pageViewCounts(2))
      shuffled <- SparkUtils.materializeShuffle[IO](spark, df)
      _ <- SparkUtils.replaceView[IO](viewName, shuffled.df)
      first <- stampsFromOneAttempt
      _ <- IO.sleep(10.millis)
      second <- stampsFromOneAttempt
    } yield (shuffled.shuffle.isDefined, first.size, second.size, first != second) must beEqualTo((true, 1, 1, true))
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
    SparkUtils.session[IO](config.spark, new DeltaWriter(delta), delta, cores = 2).use { spark =>
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

  // A window whose biggest event_name is more than a fair share is the case a hash cannot handle:
  // one name means one task, and that task becomes the commit's critical path. Splitting it and
  // bin-packing the pieces must bring every partition back near an even share, within
  // writerParallelism partitions.
  def e24 = withSpark.use { spark =>
    val viewName = "test_split_partitioning_e24"
    for {
      _ <- IO.blocking(skewedWindow(spark).createOrReplaceTempView(viewName))
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 6, skewPartitioning, skewedCounts)
      numPartitions <- IO.blocking(df.rdd.getNumPartitions)
      placements <- IO.blocking(collectPlacements(df))
      partitionSizes = placements.groupBy(_._2).view.mapValues(_.size).values.toList
      hotPartitions  = placements.collect { case ("hot", partition) => partition }.distinct.size
    } yield {
      // An even share is the floor of what any assignment into 6 partitions can achieve, so the
      // assertion is that we land close to it rather than under it.
      val evenShare = totalSkewedEvents.toDouble / 6
      (numPartitions must_== 6) and
        (placements.size must_== totalSkewedEvents) and
        (partitionSizes.max.toDouble must beLessThan(evenShare * 1.1)) and
        (hotPartitions must beGreaterThan(1))
    }
  }

  // The other half of the trade-off: a name small enough to fit in one task must not be scattered,
  // because the lake is partitioned by event_name and every extra partition holding a name costs
  // another, smaller, output file.
  def e25 = withSpark.use { spark =>
    val viewName = "test_small_name_kept_whole_e25"
    for {
      _ <- IO.blocking(skewedWindow(spark).createOrReplaceTempView(viewName))
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 6, skewPartitioning, skewedCounts)
      placements <- IO.blocking(collectPlacements(df))
      spreadOfColdNames = placements.collect { case (name, partition) if name.startsWith("cold_") => (name, partition) }.distinct
    } yield spreadOfColdNames.groupBy(_._1).values.map(_.size).toList.distinct must_== List(1)
  }

  // A vertically large loader receiving a slow trickle. A fair share is a fraction of an event
  // here, so without the minEventsPerSplit floor the two events would be split apart and written
  // as two parquet files. They share an event_name, so one file is achievable and is what we want.
  def e26 = withSpark.use { spark =>
    val viewName = "test_trickle_partitioning_e26"
    val counts   = Map(Option("hot") -> 2)
    for {
      _ <- IO.blocking(windowOf(spark, counts).createOrReplaceTempView(viewName))
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 31, tricklePartitioning, counts)
      placements <- IO.blocking(collectPlacements(df))
    } yield (placements.size must_== 2) and (placements.map(_._2).distinct.size must_== 1)
  }

  // A null event_name is packed like any other key, with no special case anywhere: 600 nameless
  // rows over 6 partitions at minEventsPerSplit 100, so a fair share is 100 and the planner cuts
  // them into 6 even pieces.
  def e27 = withSpark.use { spark =>
    val viewName = "test_nameless_partitioning_e27"
    val counts   = Map(Option.empty[String] -> 600)
    for {
      _ <- IO.blocking(windowOf(spark, counts).createOrReplaceTempView(viewName))
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 6, skewPartitioning, counts)
      numPartitions <- IO.blocking(df.rdd.getNumPartitions)
      perPartition <- IO.blocking(df.rdd.mapPartitions(rows => Iterator(rows.size)).collect().toList)
    } yield (numPartitions must_== 6) and
      (perPartition.sum must_== 600) and
      (perPartition.count(_ > 0) must_== 6)
  }

  // A `CreateArray` surviving the optimizer would allocate an ArrayData per row, on the largest
  // keys in the window, and produce identical results - so it is invisible to every other example
  // here, and this one asserts on the plan instead.
  //
  // The literal half is what stops it passing vacuously: if splitting stopped happening there would
  // be no array of either kind.
  def e28 = withSpark.use { spark =>
    val viewName = "test_partition_array_folded_e28"
    for {
      _ <- IO.blocking(skewedWindow(spark).createOrReplaceTempView(viewName))
      df <- SparkUtils.prepareFinalDataFrame[IO](spark, viewName, writerParallelism = 6, skewPartitioning, skewedCounts)
      // The repartition's own expression, not whatever else the plan holds, so this cannot pass on
      // a folded array that came from somewhere else - and fails loudly if the plan shape moves.
      expressions <- IO.blocking {
                       df.queryExecution.optimizedPlan match {
                         case r: RepartitionByExpression => r.partitionExpressions.flatMap(_.collect { case e => e })
                         case other => throw new IllegalStateException(s"Expected a repartition, got ${other.nodeName}")
                       }
                     }
      unfolded = expressions.collect { case c: CreateArray => c }
      folded   = expressions.collect { case l: Literal if l.dataType.isInstanceOf[ArrayType] => l }
    } yield (unfolded must beEmpty) and (folded must not(beEmpty))
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

  /**
   * `reference.conf`'s values, for the examples whose subject is something other than splitting.
   */
  private val defaultPartitioning =
    Config.WriterPartitioning(splitsPerFairShare = 4, minEventsPerSplit = 5000)

  // The skew fixture is only 10000 events, so it needs a floor scaled to it. Otherwise the floor
  // keeps every name whole and there is no splitting left to assert on.
  private val skewPartitioning =
    Config.WriterPartitioning(splitsPerFairShare = 4, minEventsPerSplit = 100)

  // The floor at its reference.conf default, which is what makes the trickle case interesting.
  private val tricklePartitioning =
    Config.WriterPartitioning(splitsPerFairShare = 4, minEventsPerSplit = 5000)

  /** The histogram of a batch from `eventRows`, whose rows all carry the same event_name. */
  private def pageViewCounts(numEvents: Int): Map[Option[String], Int] =
    Map(Some("page_view") -> numEvents)

  // 6000 events of one dominant name, 2000 of a second, and 2000 spread over a long tail of 20.
  // Over 6 writer threads a fair share is 1666, so "hot" and "warm" both have to be split.
  private val skewedCounts: Map[Option[String], Int] =
    Map(Option("hot") -> 6000, Option("warm") -> 2000) ++ (1 to 20).map(i => Option(f"cold_$i%02d") -> 100).toMap

  private val totalSkewedEvents: Int = skewedCounts.values.sum

  private def skewedWindow(spark: SparkSession): DataFrame =
    windowOf(spark, skewedCounts)

  /**
   * The two columns `prepareFinalDataFrame` partitions on. event_ids are derived from a counter
   * rather than random, so that the hash-based placement is identical on every run.
   */
  private def windowOf(spark: SparkSession, counts: Map[Option[String], Int]): DataFrame = {
    val rows = counts.toList.sortBy(_._1).flatMap { case (name, count) =>
      (1 to count).map { i =>
        Row(name.orNull, UUID.nameUUIDFromBytes(s"${name.getOrElse("<null>")}-$i".getBytes(StandardCharsets.UTF_8)).toString)
      }
    }
    val schema = StructType(
      Array(
        StructField("event_name", StringType, nullable = true),
        StructField("event_id", StringType, nullable   = false)
      )
    )
    spark.createDataFrame(rows.asJava, schema)
  }

  /** Every event's (event_name, spark partition index), which is what both properties are about. */
  private def collectPlacements(df: DataFrame): List[(String, Int)] =
    df.rdd
      .mapPartitionsWithIndex { case (partition, rows) => rows.map(row => (row.getAs[String]("event_name"), partition)) }
      .collect()
      .toList

  // prepareFinalDataFrame repartitions by event_name and event_id, so any schema reaching it must
  // carry both columns.
  private val eventSchema = StructType(
    Array(
      StructField("event_id", StringType, nullable   = false),
      StructField("event_name", StringType, nullable = false)
    )
  )

  private def eventRows(ids: String*): NonEmptyList[Row] =
    NonEmptyList.fromListUnsafe(ids.toList.map(id => Row(id, "page_view")))

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
        // Matches reference.conf, and load-bearing here: materializeShuffle degrades to a no-op
        // under AQE, which would leave the examples below asserting nothing.
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    Resource.make(build)(s => IO.blocking(s.close()))
  }
}
