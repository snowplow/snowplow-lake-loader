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

import org.specs2.Specification

import java.io.{FileNotFoundException, IOException}
import java.net.UnknownHostException
import java.nio.file.AccessDeniedException

import org.apache.iceberg.exceptions.RESTException

import org.apache.hadoop.fs.s3a.UnknownStoreException
import org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.model.{NoSuchBucketException, S3Exception}
import software.amazon.awssdk.services.sts.model.StsException

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.{
  AbfsRestOperationException,
  InvalidAbfsRestOperationException,
  InvalidConfigurationValueException,
  InvalidUriAuthorityException,
  TimeoutException => AbfsTimeoutException
}
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator

import com.google.api.client.googleapis.json.{GoogleJsonError, GoogleJsonResponseException}
import com.google.api.client.http.{HttpHeaders, HttpResponseException}
import com.google.cloud.storage.StorageException

import com.google.cloud.hadoop.repackaged.ossgcs.com.google.api.client.googleapis.json.{
  GoogleJsonError => RepackagedGoogleJsonError,
  GoogleJsonResponseException => RepackagedGoogleJsonResponseException
}
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.api.client.http.{
  HttpHeaders => RepackagedHttpHeaders,
  HttpResponseException => RepackagedHttpResponseException
}
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.cloud.storage.{StorageException => RepackagedStorageException}

import com.snowplowanalytics.snowplow.lakes.tables.DeltaWriter

/**
 * Rebuilds cause chains observed against real resources, and runs them through the same walk as
 * common-streams' `Retrying`: every level of the cause chain, every match collected. Any match
 * makes the error a setup error, so each case asserts the exact list of messages.
 */
class DestinationSetupErrorSpec extends Specification {
  import DestinationSetupErrorSpec._

  def is = s2"""
  DestinationSetupError.check, walked over the whole cause chain, should give:
    nothing for an s3a endpoint that does not resolve $s3UnknownEndpoint
    nothing for an Iceberg REST catalog host that does not resolve $restUnknownHost
    only "Wrong storage name" for an ABFS account that does not resolve $abfsUnknownAccount
    only "Wrong storage name" for an ABFS account that fails DeltaWriter's host check $abfsUnresolvedHost
    only the tenant message for an unknown Azure tenant $abfsBadTenant
    only the client secret message for a wrong Azure client secret $abfsWrongSecret
    only the Azure 403 message for an ABFS 403 $abfsForbidden
    only the Azure 404 message for a missing ABFS container $abfsMissingContainer
    only "Invalid storage container path" for a blob endpoint URL $abfsBlobEndpoint
    only "Invalid storage container path" for a URL without a container $abfsNoContainer
    only the 400 message, with Azure's description, for a bad container name $abfsBadContainerName
    today's messages for an S3 403 $s3Forbidden
    today's messages for a denied assume-role $s3AssumeRoleDenied
    today's messages for a missing S3 bucket $s3MissingBucket
    today's message for a GCS key of a nonexistent service account (hadoop-gcp) $gcsHadoopFakeKeyfile
    today's messages for a GCS 403 (hadoop-gcp) $gcsHadoopForbidden
    today's messages for a missing GCS bucket (hadoop-gcp) $gcsHadoopMissingBucket
    today's messages for a GCS 403 (Iceberg GCSFileIO) $gcsIcebergForbidden
    nothing for the ABFS HNS check wrapping a timeout $abfsHnsTimeout
    the generic Azure configuration message for a non-numeric integer setting $abfsBadIntegerSetting
  """

  def s3UnknownEndpoint = {
    val uhe = new UnknownHostException("snowplow-probe-bucket.snowplowprobe12d8190b.invalid")
    val sdk2 = SdkClientException
      .builder()
      .message("Unable to execute HTTP request: snowplow-probe-bucket.snowplowprobe12d8190b.invalid (SDK Attempt Count: 2)")
      .cause(uhe)
      .build()
    val sdk1 = SdkClientException
      .builder()
      .message(
        "Received an UnknownHostException when attempting to interact with a service. See cause for the exact endpoint that is failing to resolve. If this is happening on an endpoint that previously worked, there may be a network connectivity issue or your DNS cache could be storing endpoints for too long."
      )
      .cause(sdk2)
      .build()
    val top = new UnknownHostException(s"getFileStatus on s3a://snowplow-probe-bucket/lake/_delta_log: $sdk1: $sdk1")
    top.initCause(sdk1)
    walk(top) must beEmpty
  }

  // Only the destination triage: for REST catalog targets, TableFormatSetupError.IcebergRest
  // separately classifies this as a setup error ("REST catalog URI isn't reachable")
  def restUnknownHost = {
    val uhe = new UnknownHostException("snowplowprobe12d8190b.invalid: nodename nor servname provided, or not known")
    walk(new RESTException(uhe, "Error occurred while processing %s request", "GET")) must beEmpty
  }

  def abfsUnknownAccount = {
    val uhe   = new UnknownHostException("snowplowprobe12d8190b.dfs.core.windows.net")
    val inner = new InvalidAbfsRestOperationException(uhe)
    val top   = new InvalidConfigurationValueException("fs.azure.account.hns.enabled", inner)
    walk(top) must_== List("Wrong storage name")
  }

  def abfsUnresolvedHost = {
    val uhe = new UnknownHostException("snowplowprobe12d8190b.dfs.core.windows.net: Name or service not known")
    walk(new DeltaWriter.UnresolvedStorageHostException("snowplowprobe12d8190b.dfs.core.windows.net", uhe)) must_== List(
      "Wrong storage name"
    )
  }

  def abfsBadTenant = {
    val top = hnsWrapper(
      aadFailure(
        400,
        """{"error":"invalid_request","error_description":"AADSTS90002: Tenant '7f16821a-b364-42c7-b4f8-b6f70c075791' not found. Check to make sure you have the correct tenant ID and are signing into the correct cloud.","error_codes":[90002]}"""
      )
    )
    walk(top) must_== List("Could not authenticate. Tenant is invalid or does not have an active subscription")
  }

  def abfsWrongSecret = {
    val top = hnsWrapper(
      aadFailure(
        401,
        """{"error":"invalid_client","error_description":"AADSTS7000215: Invalid client secret provided. Ensure the secret being sent in the request is the client secret value, not the client secret ID, for a secret added to app 'fcc5e451-591f-4aa3-bfb1-fa5f10c9c7c9'.","error_codes":[7000215]}"""
      )
    )
    walk(top) must_== List("Could not authenticate. Invalid client secret provided")
  }

  def abfsForbidden = {
    val message =
      """Operation failed: "This request is not authorized to perform this operation using this permission.", 403, HEAD, https://llprobebd96b39a.dfs.core.windows.net/events/probe/_delta_log?upn=false&action=getStatus&timeout=90s, rId: 31c75094-701f-00a6-0bb6-600d63000000"""
    val abfs = new AbfsRestOperationException(403, "UNKNOWN", message, null)
    val top  = new AccessDeniedException(message)
    top.initCause(abfs)
    walk(top) must_== List(
      "Missing permissions for the destination (needs \"Storage Blob Data Contributor\" assigned to the service principal for the container)"
    )
  }

  def abfsMissingContainer = {
    val message =
      """Operation failed: "The specified filesystem does not exist.", 404, PUT, https://llprobebd96b39a.dfs.core.windows.net/no-such-container-snowplowprobeec7ff191/probe/_delta_log?resource=directory&timeout=90, rId: 31893c2f-c01f-00b3-5bb6-601ad0000000, FilesystemNotFound, "The specified filesystem does not exist. RequestId:31893c2f-c01f-00b3-5bb6-601ad0000000 Time:2026-10-20T17:13:54.6691121Z""""
    val abfs = new AbfsRestOperationException(404, "FilesystemNotFound", message, null)
    val top  = new FileNotFoundException(message)
    top.initCause(abfs)
    walk(top) must_== List("The specified filesystem does not exist (e.g. wrong container name)")
  }

  def abfsBlobEndpoint = {
    val top = new InvalidConfigurationValueException(
      "fs.defaultFS",
      "Blob Endpoint Url Cannot be used to initialize filesystem for HNS Account"
    )
    (top.getMessage must startWith("Invalid configuration value detected for \"fs.defaultFS\"")) and
      (walk(top) must_== List("Invalid storage container path"))
  }

  def abfsNoContainer = {
    val top = new InvalidUriAuthorityException("abfss://snowplowprobe.dfs.core.windows.net/lake")
    walk(top) must_== List("Invalid storage container path")
  }

  def abfsBadContainerName = {
    val message =
      """Operation failed: "The specifed resource name contains invalid characters.", 400, HEAD, https://llprobe6d84a8ce.blob.core.windows.net/Events_Bad/probe/_delta_log?upn=false&timeout=90s, rId: b4e44ea8-a01e-0066-74b7-60943d000000"""
    val top = new AbfsRestOperationException(400, "UNKNOWN", message, null)
    walk(top) must_== List("Azure storage rejected the request: The specifed resource name contains invalid characters.")
  }

  def s3Forbidden = {
    val s3 = S3Exception
      .builder()
      .message("Forbidden")
      .statusCode(403)
      .requestId("D96E2V455SHT053M")
      .awsErrorDetails(AwsErrorDetails.builder().serviceName("S3").build())
      .build()
    val top = new AccessDeniedException(
      "s3a://ll-probe-c812c02a/probe/_delta_log",
      null,
      s"getFileStatus on s3a://ll-probe-c812c02a/probe/_delta_log: $s3:null"
    )
    top.initCause(s3)
    walk(top) must_== List(
      "s3a://ll-probe-c812c02a/probe/_delta_log: getFileStatus on s3a://ll-probe-c812c02a/probe/_delta_log",
      "Missing permission on the S3 bucket"
    )
  }

  def s3AssumeRoleDenied = {
    val sts = StsException
      .builder()
      .message(
        "User: arn:aws:sts::<account-id>:assumed-role/<sso-role>/<user> is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::<account-id>:role/ll-probe-rw"
      )
      .statusCode(403)
      .requestId("6bd7f0a7-67df-4ea6-979b-34dace716f9d")
      .awsErrorDetails(AwsErrorDetails.builder().serviceName("Sts").build())
      .build()
    val noAuth = new NoAuthWithAWSException(s"No AWS Credentials provided by AssumedRoleCredentialsProvider : $sts", sts)
    val top    = new AccessDeniedException("s3a://ll-probe-c812c02a/probe/_delta_log", null, noAuth.toString)
    top.initCause(noAuth)
    walk(top) must_== List(
      "s3a://ll-probe-c812c02a/probe/_delta_log",
      "Failed to initialize AWS access credentials",
      "Missing permissions to assume the AWS IAM role"
    )
  }

  def s3MissingBucket = {
    val noSuchBucket = NoSuchBucketException
      .builder()
      .message("The specified bucket does not exist")
      .statusCode(404)
      .requestId("ZBSSRR5Z7H4CH3VT")
      .awsErrorDetails(AwsErrorDetails.builder().serviceName("S3").errorCode("NoSuchBucket").build())
      .build()
    val top = new UnknownStoreException(
      "s3a://snowplowprobe97352172-bucket/probe/_delta_log",
      s"getFileStatus on s3a://snowplowprobe97352172-bucket/probe/_delta_log: $noSuchBucket:NoSuchBucket: ${noSuchBucket.getMessage}",
      noSuchBucket
    )
    walk(top) must_== List(
      "`s3a://snowplowprobe97352172-bucket/probe/_delta_log': getFileStatus on s3a://snowplowprobe97352172-bucket/probe/_delta_log",
      "S3 bucket does not exist or we do not have permissions to see it exists"
    )
  }

  def gcsHadoopFakeKeyfile = {
    val content =
      """{"error":"invalid_grant","error_description":"Invalid grant: account not found"}"""
    val http = new RepackagedHttpResponseException.Builder(400, "Bad Request", new RepackagedHttpHeaders())
      .setContent(content)
      .setMessage(s"400 Bad Request\nPOST https://oauth2.googleapis.com/token\n$content")
      .build()
    val authMessage =
      s"Error getting access token for service account: 400 Bad Request\nPOST https://oauth2.googleapis.com/token\n$content, iss: nobody@snowplow-probe.iam.gserviceaccount.com"
    val auth = repackagedGoogleAuthException(authMessage, http)
    val top  = new RepackagedStorageException(0, authMessage, auth)
    walk(top) must_== List("The service account key is invalid")
  }

  def gcsHadoopForbidden = {
    val message =
      "ll-probe-norole@engineering-sandbox.iam.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket. Permission 'storage.objects.list' denied on resource '//storage.googleapis.com/projects/_/buckets/ll-probe-40c13343' (or it may not exist)."
    val top = new RepackagedStorageException(403, message, repackagedJsonResponse(403, "Forbidden", message))
    walk(top) must_== List(message, message)
  }

  def gcsHadoopMissingBucket = {
    val message = "The specified bucket does not exist."
    val top     = new RepackagedStorageException(404, message, repackagedJsonResponse(404, "Not Found", message))
    walk(top) must_== List("The specified bucket does not exist", "The specified bucket does not exist")
  }

  def gcsIcebergForbidden = {
    val message =
      "ll-probe-norole@engineering-sandbox.iam.gserviceaccount.com does not have storage.objects.get access to the Google Cloud Storage object. Permission 'storage.objects.get' denied on resource '//storage.googleapis.com/projects/_/buckets/ll-probe-40c13343/objects/probe/metadata/probe-dfa1b052-640a-4a86-b1a7-3dad8cac6f50.json' (or it may not exist)."
    val details = new GoogleJsonError()
    details.setCode(403)
    details.setMessage(message)
    val json = new GoogleJsonResponseException(
      new HttpResponseException.Builder(403, "Forbidden", new HttpHeaders()).setMessage(s"403 Forbidden\n$message"),
      details
    )
    val top = new StorageException(403, message, json)
    walk(top) must_== List("IAM role is missing permissions", message)
  }

  def abfsHnsTimeout = {
    val top = new InvalidConfigurationValueException(
      "fs.azure.account.hns.enabled",
      new AbfsTimeoutException("Operation timed out")
    )
    walk(top) must beEmpty
  }

  def abfsBadIntegerSetting = {
    val top = new InvalidConfigurationValueException(
      "fs.azure.io.retry.max.retries",
      new NumberFormatException("For input string: \"lots\"")
    )
    walk(top) must_== List("Invalid Azure storage configuration: Invalid configuration value detected for fs.azure.io.retry.max.retries")
  }
}

object DestinationSetupErrorSpec {

  /** The same walk as common-streams' `Retrying.checkingNestedExceptions` */
  def walk(t: Throwable): List[String] = {
    def go(t: Throwable, acc: List[Throwable]): List[Throwable] =
      Option(t.getCause) match {
        case Some(cause) => go(cause, cause :: acc)
        case None        => acc.reverse
      }
    go(t, List(t)).flatMap(DestinationSetupError.check.lift)
  }

  /** hadoop-azure's HNS check wraps any failure in this, in `AzureBlobFileSystem.initialize` */
  private def hnsWrapper(cause: Exception): InvalidConfigurationValueException =
    new InvalidConfigurationValueException("fs.azure.account.hns.enabled", cause)

  /** A rejection from the Azure AD token endpoint, as hadoop-azure builds it */
  private def aadFailure(httpCode: Int, body: String): AbfsRestOperationException = {
    val url = "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
    val http = new AzureADAuthenticator.HttpException(
      httpCode,
      "968a7f99-41dc-49ad-a273-0845c6418600",
      s"AADToken: HTTP connection to $url failed for getting token from AzureAD.",
      url,
      "application/json; charset=utf-8",
      body
    )
    new AbfsRestOperationException(-1, "UNKNOWN", s"Auth failure: ${http.getMessage}", http)
  }

  private def repackagedJsonResponse(
    code: Int,
    status: String,
    message: String
  ): RepackagedGoogleJsonResponseException = {
    val details = new RepackagedGoogleJsonError()
    details.setCode(code)
    details.setMessage(message)
    new RepackagedGoogleJsonResponseException(
      new RepackagedHttpResponseException.Builder(code, status, new RepackagedHttpHeaders()).setMessage(s"$code $status\n$message"),
      details
    )
  }

  /** google-auth-library's GoogleAuthException is package-private, so it is built reflectively */
  private def repackagedGoogleAuthException(message: String, cause: Throwable): IOException = {
    val clazz = Class.forName("com.google.cloud.hadoop.repackaged.ossgcs.com.google.auth.oauth2.GoogleAuthException")
    val ctor  = clazz.getDeclaredConstructor(classOf[Boolean], classOf[Int], classOf[String], classOf[Throwable])
    ctor.setAccessible(true)
    ctor.newInstance(Boolean.box(false), Int.box(0), message, cause).asInstanceOf[IOException]
  }
}
