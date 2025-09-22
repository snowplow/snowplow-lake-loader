/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.implicits._
import cats.effect.IO
import org.apache.hadoop.fs.s3a.{CredentialInitializationException, UnknownStoreException}
import software.amazon.awssdk.services.s3.model.{NoSuchBucketException, S3Exception}
import software.amazon.awssdk.services.sts.model.StsException
import software.amazon.awssdk.services.glue.model.{
  AccessDeniedException => GlueAccessDeniedException,
  EntityNotFoundException => GlueEntityNotFoundException
}
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException

import java.nio.file.AccessDeniedException
import scala.util.matching.Regex

import com.snowplowanalytics.snowplow.streams.kinesis.{KinesisFactory, KinesisSinkConfig, KinesisSourceConfig}

object AwsApp extends LoaderApp[Unit, KinesisSourceConfig, KinesisSinkConfig](BuildInfo) {

  override def toFactory: FactoryProvider = _ => KinesisFactory.resource[IO]

  /**
   * Identifies known exceptions relating to setup of the destination
   *
   * Exceptions are often "caused by" an underlying exception. For example, a s3a
   * UnknownStoreException is often "caused by" a aws sdk NoSuchBucketException. Our implementation
   * checks both the top exception and the underlying causes. Therefore in some cases we
   * over-specify the exceptions to watch out for; the top exception and causal exception both match
   */
  override def isDestinationSetupError: DestinationSetupErrorCheck = {

    // Exceptions raised by underlying AWS SDK
    case _: NoSuchBucketException =>
      // S3 bucket does not exist
      "S3 bucket does not exist or we do not have permissions to see it exists"
    case e: S3Exception if e.statusCode() === 403 =>
      // No permission to read from S3 bucket or to write to S3 bucket
      extractMissingS3Permission(e)
    case e: S3Exception if e.statusCode() === 301 =>
      // Misconfigured AWS region
      "S3 bucket is not in the expected region"
    case e: GlueAccessDeniedException =>
      // No permission to read from Glue catalog
      extractMissingGluePermission(e)
    case _: GlueEntityNotFoundException =>
      // Glue database does not exist
      "Glue resource does not exist or no permission to see it exists"
    case e: StsException if e.statusCode() === 403 =>
      // No permission to assume the role given to authenticate to S3/Glue
      "Missing permissions to assume the AWS IAM role"

    // Exceptions raised via hadoop's s3a filesystem
    case e: UnknownStoreException =>
      // S3 bucket does not exist or no permission to see it exists
      stripCauseDetails(e)
    case e: AccessDeniedException =>
      // 1 - No permission to put object on the bucket
      // 2 - No permission to assume the role given to authenticate to S3
      stripCauseDetails(e)
    case _: CredentialInitializationException =>
      "Failed to initialize AWS access credentials"

    // Exceptions raised by underlying AWS SDK v1.
    // Note AWS v1 dependencies will be removed in the next release of Hadoop and Delta
    case e: AmazonDynamoDBException if e.getErrorCode === "AccessDeniedException" =>
      s"Missing permissions to operate on the DynamoDB table"
    case e: AmazonDynamoDBException if e.getErrorCode === "ValidationException" =>
      s"DynamoDB table does not have the expected structure"

    // Exceptions common to the table format - Delta/Iceberg/Hudi
    case TableFormatSetupError.check(t) =>
      t
  }

  /**
   * Fixes hadoop Exception messages to be more reader-friendly
   *
   * Hadoop exception messages often add the exception's cause to the exception's message.
   *
   * E.g. "<HELPFUL MESSAGE>: <CAUSE CLASSNAME>: <CAUSE MESSAGE>"
   *
   * In order to have better control of the message sent to the webhook, we remove the cause details
   * here, and add back in pertinent cause information later.
   */
  private def stripCauseDetails(t: Throwable): String =
    Option(t.getCause) match {
      case Some(cause) =>
        val toRemove = new Regex(":? *" + Regex.quote(cause.toString) + ".*")
        toRemove.replaceAllIn(t.getMessage, "")
      case None =>
        t.getMessage
    }

  /**
   * Extracts s3:ActionName from exception message if possible
   *
   * Example exception message is as below: User: arn:aws:sts::...loader is not authorized to
   * perform: s3:PutObject on resource: "arn:aws:s3:::bucket-name/....json" because no
   * identity-based policy allows the s3:PutObject action (Service: S3, Status Code: 403, Request
   * ID: req-id, Extended Request ID: ext-req-id)
   */
  private def extractMissingS3Permission(s3Exception: S3Exception): String = {
    val pattern = """.*is not authorized to perform: s3:(\w+).*""".r
    s3Exception.getMessage match {
      case pattern(action) =>
        s"Missing s3:$action permission on the S3 bucket"
      case _ =>
        "Missing permission on the S3 bucket"
    }
  }

  /**
   * Extracts glue:ActionName from exception message if possible
   */
  private def extractMissingGluePermission(glueException: GlueAccessDeniedException): String = {
    val pattern = """.*is not authorized to perform: glue:(\w+).*""".r
    Option(glueException.getMessage) match {
      case Some(pattern(action)) =>
        s"Missing glue:$action permission on the Glue catalog"
      case _ =>
        "Missing permission on the Glue catalog"
    }
  }
}
