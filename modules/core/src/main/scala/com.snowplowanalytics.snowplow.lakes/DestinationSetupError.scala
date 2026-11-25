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

import java.net.UnknownHostException
import java.nio.file.AccessDeniedException
import scala.util.matching.Regex

// AWS
import org.apache.hadoop.fs.s3a.{CredentialInitializationException, UnknownStoreException}
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.s3.model.{NoSuchBucketException, S3Exception}
import software.amazon.awssdk.services.sts.model.StsException
import software.amazon.awssdk.services.glue.model.{
  AccessDeniedException => GlueAccessDeniedException,
  EntityNotFoundException => GlueEntityNotFoundException
}
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException

// Azure
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.{
  AbfsRestOperationException,
  AzureBlobFileSystemException,
  InvalidConfigurationValueException,
  InvalidUriAuthorityException
}

// GCP: thrown by Iceberg's GCSFileIO (Iceberg REST catalogs on GCS), which uses the plain
// java-storage/api-client libraries
import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.google.cloud.storage.StorageException

// GCP: thrown by hadoop-gcp (the gs scheme for Delta and Hadoop-catalog Iceberg): it ships only as a
// shaded jar that relocates the same libraries, so its exceptions are these repackaged classes
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.api.client.googleapis.json.{
  GoogleJsonResponseException => RepackagedGoogleJsonResponseException
}
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.api.client.http.{HttpResponseException => RepackagedHttpResponseException}
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.cloud.storage.{StorageException => RepackagedStorageException}

import com.snowplowanalytics.snowplow.lakes.tables.DeltaWriter

/**
 * Identifies known exceptions relating to setup of the destination
 *
 * Every image can write to every store (S3, ABFS and GCS), whichever cloud it is named for, so a
 * single check covers all three. The arms for different stores must not overlap: tell them apart by
 * how the exceptions are built (type, status code, cause) rather than by message text.
 */
object DestinationSetupError {

  def check: DestinationSetupErrorCheck =
    Aws.check.orElse(Azure.check).orElse(Gcp.check)

  object Aws {

    /**
     * Exceptions are often "caused by" an underlying exception. For example, a s3a
     * UnknownStoreException is often "caused by" a aws sdk NoSuchBucketException. Our application
     * checks both the top exception and the underlying causes. Therefore in some cases we
     * over-specify the exceptions to watch out for; the top exception and causal exception both
     * match
     */
    def check: DestinationSetupErrorCheck = {
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
      // Unlike every other arm, this one looks below its own level of the cause chain. hadoop-azure
      // throws the same JDK exception for an ABFS 403, so only an AWS SDK exception beneath it shows
      // this came from s3a. It checks every cause rather than only the immediate one, so it still
      // holds if s3a wraps the SDK exception in an IOException of its own.
      case e: AccessDeniedException if hasSdkExceptionInCauses(e) =>
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
    }

    private def hasSdkExceptionInCauses(t: Throwable): Boolean =
      Iterator
        .iterate(t.getCause)(_.getCause)
        .takeWhile(_ != null)
        .take(100) // guards against a cyclic cause chain
        .exists(_.isInstanceOf[SdkException])

    /**
     * Fixes hadoop Exception messages to be more reader-friendly
     *
     * Hadoop exception messages often add the exception's cause to the exception's message.
     *
     * E.g. "<HELPFUL MESSAGE>: <CAUSE CLASSNAME>: <CAUSE MESSAGE>"
     *
     * In order to have better control of the message sent to the webhook, we remove the cause
     * details here, and add back in pertinent cause information later.
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

  object Azure {

    def check: DestinationSetupErrorCheck = {
      // Authentication issue (wrong OAuth endpoint, wrong client id, wrong secret)
      case AuthenticationError(e) =>
        e
      // Storage account host does not resolve, caught by DeltaWriter's own check before hadoop-azure
      case _: DeltaWriter.UnresolvedStorageHostException =>
        "Wrong storage name"
      // Storage account host does not resolve, from hadoop-azure itself
      case e: AbfsRestOperationException if e.getStatusCode === -1 && e.getCause.isInstanceOf[UnknownHostException] =>
        "Wrong storage name"
      // Bad request, e.g. a container name with invalid characters. Retrying cannot succeed.
      case e: AbfsRestOperationException if e.getStatusCode === 400 =>
        extractAzureDescription(e) match {
          case Some(description) => s"Azure storage rejected the request: $description"
          case None              => "Azure storage rejected the request as invalid"
        }
      // Wrong container name
      case e: AbfsRestOperationException if e.getStatusCode === 404 =>
        s"The specified filesystem does not exist (e.g. wrong container name)"
      // Service principal missing permissions for container (role assignement missing or wrong role)
      case e: AbfsRestOperationException if e.getStatusCode === 403 =>
        s"Missing permissions for the destination (needs \"Storage Blob Data Contributor\" assigned to the service principal for the container)"
      // Soft delete not disabled
      case e: AbfsRestOperationException if e.getStatusCode === 409 =>
        "Blob soft delete must be disabled on the storage account"
      // A real configuration error has no cause or wraps a parse error. hadoop-azure also wraps any
      // failure of its HNS check (auth, DNS, timeout) in this exception, keyed
      // fs.azure.account.hns.enabled, with an AzureBlobFileSystemException as the cause: those are
      // matched, or deliberately not matched, at the level of that cause.
      case e: InvalidConfigurationValueException if !e.getCause.isInstanceOf[AzureBlobFileSystemException] =>
        if (Option(e.getMessage).exists(_.contains("fs.defaultFS")))
          "Invalid storage container path"
        else
          s"Invalid Azure storage configuration: ${e.getMessage}"
      // The storage container path does not name a container
      case _: InvalidUriAuthorityException =>
        "Invalid storage container path"
    }

    private val azureDescriptionPattern = """Operation failed: "([^"]+)"""".r.unanchored

    /**
     * Extracts Azure's own description of the error, which hadoop-azure quotes at the start of the
     * message, e.g. `Operation failed: "The specifed resource name contains invalid characters.",
     * 400, HEAD, ...`
     */
    private def extractAzureDescription(e: AbfsRestOperationException): Option[String] =
      Option(e.getMessage).collect { case azureDescriptionPattern(description) =>
        description
      }
  }

  object Gcp {

    def check: DestinationSetupErrorCheck = {
      // --- Plain classes, thrown on the Iceberg GCSFileIO path ---
      // Bad Request - Key belongs to nonexistent service account
      case e: TokenResponseException if e.getStatusCode === 400 =>
        "The service account key is invalid"
      // Same condition surfaced via google-auth-library. Its GoogleAuthException is package-private,
      // so match the underlying HTTP rejection: the invalid_grant OAuth error identifies a rejected
      // credential, e.g. a key belonging to a deleted service account
      case e: HttpResponseException if e.getStatusCode === 400 && Option(e.getContent).exists(_.contains("invalid_grant")) =>
        "The service account key is invalid"
      // Forbidden - permissions missing for Cloud Storage
      case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(403) =>
        Option(e.getDetails.getMessage).getOrElse("IAM role is missing permissions")
      // Not Found - destination bucket doesn't exist
      case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(404) =>
        "The specified bucket does not exist"
      // Forbidden - permissions missing for Cloud Storage
      case e: StorageException if e.getCode === 403 =>
        "IAM role is missing permissions"

      // --- Repackaged equivalents, thrown on the hadoop-gcp path ---
      // Bad Request - the OAuth token endpoint rejected the credential
      case e: RepackagedHttpResponseException if e.getStatusCode === 400 && Option(e.getContent).exists(_.contains("invalid_grant")) =>
        "The service account key is invalid"
      // Not Found - destination bucket doesn't exist
      case e: RepackagedStorageException if e.getCode === 404 =>
        "The specified bucket does not exist"
      // Forbidden - permissions missing for Cloud Storage
      case e: RepackagedStorageException if e.getCode === 403 =>
        Option(e.getMessage).getOrElse("IAM role is missing permissions")
      // On the paths exercised so far the HTTP-transport rejection appears beneath a translated
      // RepackagedStorageException, which the arms above match first. Kept for paths where the
      // java-storage wrapper may be absent (the cause-chain walker visits every level either way)
      case e: RepackagedGoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(404) =>
        "The specified bucket does not exist"
      case e: RepackagedGoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(403) =>
        Option(e.getDetails.getMessage).getOrElse("IAM role is missing permissions")
    }
  }
}
