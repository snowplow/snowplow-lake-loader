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
import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.spark.sql.{DataFrame, Row, SnowplowInternalSparkBridge, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.Writer
import com.snowplowanalytics.snowplow.lakes.fs.LakeLoaderFileSystem

import scala.jdk.CollectionConverters._

private[processing] object SparkUtils {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def session[F[_]: Async](
    config: Config.Spark,
    writer: Writer,
    target: Config.Target
  ): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .master(s"local[*, ${config.taskRetries}]")
        .config(sparkConfigOptions(config, writer))

    val openLogF  = Logger[F].info("Creating the global spark session...")
    val closeLogF = Logger[F].info("Closing the global spark session...")
    val buildF    = Sync[F].delay(builder.getOrCreate())

    Resource
      .make(openLogF >> buildF)(s => closeLogF >> Sync[F].blocking(s.close()))
      .evalTap { session =>
        target match {
          case delta: Config.Delta =>
            Sync[F].delay {
              // Forces Spark to use `LakeLoaderFileSystem` when writing to the Lake via Hadoop
              // Delta tolerates async deletes; in other words when we delete a file, there is no strong
              // requirement that the file must be deleted immediately. Delta uses unique file names and never
              // re-writes a file that was previously deleted
              LakeLoaderFileSystem.overrideHadoopFileSystemConf(delta.location, session.sparkContext.hadoopConfiguration)
            }
          case _ => Sync[F].unit
        }
      }
  }

  val gcpUserAgent: String = "Google/ISV Solution (GPN:isol_plb32_0014m00002tg62aqad_qufvepzajwfju6jxl5uwg2zn3l3nizte)"

  private def sparkConfigOptions(config: Config.Spark, writer: Writer): Map[String, String] = {
    val gcpUserAgentKey = "fs.gs.storage.http.headers.user-agent"
    writer.sparkConfig ++ config.conf + (gcpUserAgentKey -> gcpUserAgent)
  }

  def initializeLocalDataFrame[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Initializing local DataFrame with name $viewName")
      _ <- Sync[F].blocking {
             try {
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
               spark.emptyDataFrame.createTempView(viewName)
             } finally
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
           }
    } yield ()

  def localAppendRows[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    rows: NonEmptyList[Row],
    igluSchema: StructType,
    shouldRestoreNullability: Boolean
  ): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Saving batch of ${rows.size} events to local DataFrame $viewName")
      _ <- Sync[F].blocking {
             try {
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
               val accumulatedSchema = spark.table(viewName).schema
               val united = spark
                 .createDataFrame(rows.toList.asJava, igluSchema)
                 .coalesce(1)
                 .localCheckpoint()
                 .unionByName(spark.table(viewName), allowMissingColumns = true)
               val result = if (shouldRestoreNullability) restoreNullability(igluSchema, accumulatedSchema, united) else united
               result.createOrReplaceTempView(viewName)
             } finally
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
           }
    } yield ()

  def prepareFinalDataFrame[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    writerParallelism: Int,
    writerExpectsSortedDataframe: Boolean
  ): F[DataFrame] =
    for {
      df <- Sync[F].pure(spark.table(viewName))
      df <- Sync[F].pure {
              // Create equally-balanced partitions, for which events with similar event_name are likely to be in the same partition.
              // This maximizes output file sizes, for a lake which is partitioned by event_name.
              if (writerParallelism > 1) df.repartitionByRange(writerParallelism, col("event_name"), col("event_id")) else df.coalesce(1)
            }
      df <- Sync[F].pure(if (writerExpectsSortedDataframe) df.sortWithinPartitions("event_name") else df)
    } yield df.withColumn("load_tstamp", current_timestamp())

  // Spark's unionByName can incorrectly promote inner StructType fields to nullable when the two
  // DataFrames have different nested struct schemas (e.g. after an Iglu schema patch version adds a
  // new sub-field). Spark's Cast rejects nullable → non-null casts, so we cannot use withColumn+cast
  // to fix the metadata. Instead we reattach the corrected schema via internalCreateDataFrame,
  // bypassing both Spark's analysis checks and the InternalRow→Row→InternalRow roundtrip that the
  // public createDataFrame(rdd: RDD[Row], schema) API incurs (nullability is metadata-only: it is
  // never enforced at runtime).
  private def restoreNullability(
    igluSchema: StructType,
    accumulatedSchema: StructType,
    df: DataFrame
  ): DataFrame =
    if (igluSchema == df.schema) df // union did not widen any types; nothing to correct
    else {
      val corrected = restoreNullabilityInSchema(igluSchema, accumulatedSchema, df.schema)
      if (corrected == df.schema) df
      else SnowplowInternalSparkBridge.reattachSchema(df, corrected)
    }

  // Builds the corrected top-level schema for the union result.
  // For columns that appear in the Iglu schema (source of truth for the current batch),
  // nullability is corrected recursively via restoreNullabilityInField. Extra columns that only
  // appear in the union result are left untouched.
  private def restoreNullabilityInSchema(
    igluSchema: StructType,
    accumulatedSchema: StructType,
    unionSchema: StructType
  ): StructType = {
    val igluFieldsByName        = igluSchema.fields.map(f => f.name -> f).toMap
    val accumulatedFieldsByName = accumulatedSchema.fields.map(f => f.name -> f).toMap
    StructType(unionSchema.fields.map { unionField =>
      igluFieldsByName.get(unionField.name) match {
        case Some(igluField) =>
          unionField.copy(dataType =
            restoreNullabilityInField(igluField.dataType, accumulatedFieldsByName.get(unionField.name).map(_.dataType), unionField.dataType)
          )
        case None => unionField
      }
    })
  }

  // Returns a DataType that preserves the structure of `dfType` (which may contain extra
  // accumulated nested fields not present in `igluType`) but restores nullability from `igluType`
  // and `accumulatedType` for any fields present in all. StructType and ArrayType are handled
  // recursively; all other types are returned unchanged.
  private def restoreNullabilityInField(
    igluType: DataType,
    accumulatedType: Option[DataType],
    dfType: DataType
  ): DataType =
    (igluType, dfType) match {
      case (igluStruct: StructType, dfStruct: StructType) =>
        restoreNullabilityInStruct(igluStruct, accumulatedType.collect { case st: StructType => st }, dfStruct)
      case (ArrayType(igluArrayType, igluContainsNull), ArrayType(dfArrayType, _)) =>
        val accumulatedArray = accumulatedType.collect { case at: ArrayType => at }
        ArrayType(
          restoreNullabilityInField(igluArrayType, accumulatedArray.map(_.elementType), dfArrayType),
          igluContainsNull || accumulatedArray.exists(_.containsNull)
        )
      case _ => dfType
    }

  // Produces a StructType whose fields come from `dfStruct` (the union result, which may have extra
  // fields from the accumulated view), but where inner-field nullability is restored from `igluStruct`
  // and `accumulatedStruct` for fields that appear in all.
  // nullable = igluField.nullable || accumulatedField.nullable prevents incorrectly marking a field as
  // NOT NULL when the accumulated view already contains nulls for it (e.g. a prior batch processed a
  // more permissive schema version where the field was nullable).
  private def restoreNullabilityInStruct(
    igluStruct: StructType,
    accumulatedStruct: Option[StructType],
    dfStruct: StructType
  ): StructType = {
    val igluFieldsByName        = igluStruct.fields.map(f => f.name -> f).toMap
    val accumulatedFieldsByName = accumulatedStruct.map(_.fields.map(f => f.name -> f).toMap).getOrElse(Map.empty)
    StructType(dfStruct.fields.map { dfField =>
      igluFieldsByName.get(dfField.name) match {
        case Some(igluField) =>
          val accumulatedField = accumulatedFieldsByName.get(dfField.name)
          dfField.copy(
            dataType = restoreNullabilityInField(igluField.dataType, accumulatedField.map(_.dataType), dfField.dataType),
            nullable = igluField.nullable || accumulatedField.exists(_.nullable)
          )
        case None => dfField
      }
    })
  }

  def dropView[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    Logger[F].info(s"Removing Spark data frame $viewName from local disk...") >>
      Sync[F].blocking {
        try {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
          spark.catalog.dropTempView(viewName)
        } finally
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
      }.void
}
