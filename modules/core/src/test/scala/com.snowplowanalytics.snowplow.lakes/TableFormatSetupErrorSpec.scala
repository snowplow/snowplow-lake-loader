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

import org.apache.iceberg.exceptions.NoSuchNamespaceException
import org.apache.iceberg.rest.ErrorHandlers
import org.apache.iceberg.rest.responses.ErrorResponse
import org.specs2.Specification

import com.snowplowanalytics.snowplow.lakes.tables.IcebergWriter

class TableFormatSetupErrorSpec extends Specification {
  def is = s2"""
  TableFormatSetupError.IcebergRest.check should:
    not classify a rate-limiting 429 from the REST catalog as a setup error $e1
    not classify a transient 408 / 502 / 504 from the REST catalog as a setup error $e2
    still classify a non-transient "Unable to process" error as a setup error $e3
    give a specific message when the catalog rejects a write for metadata size $e4
    classify a BigLake permission denial as a setup error via the shared REST-spec 403 message $e5
    give a specific message for a BigLake project-not-found error $e6
    classify an unusable Snowflake private key as a setup error $e7
    not classify a missing Snowflake key-pair auth option as a setup error $e8
    classify a Databricks table-level permission denial as missing privileges $e9
    give a specific message when Databricks disables the REST catalog API for the loader's credentials $e10
    not classify a 403 with an unrecognised message as a setup error $e11
    give a fixed message, without the namespace or request id, for a missing namespace $e12
    classify a table loaded without vended credentials as a setup error $e13
  """

  def e1 = TableFormatSetupError.IcebergRest.check(restException(429, "")) must beNone

  def e2 =
    (TableFormatSetupError.IcebergRest.check(restException(408, "")) must beNone) and
      (TableFormatSetupError.IcebergRest.check(restException(502, "")) must beNone) and
      (TableFormatSetupError.IcebergRest.check(restException(504, "")) must beNone)

  // 405 falls through to the generic "Unable to process" RESTException but is not transient, so it
  // is still a setup error. 500 is deliberately not used here -- it maps to ServiceFailureException
  // and never reaches this arm.
  def e3 = TableFormatSetupError.IcebergRest.check(restException(405, "method not allowed")) must beSome

  // A 422 "metadata too large" carries "Unable to process" in the message, so it would otherwise land
  // on the generic "check your REST catalog configuration" fallback. It should get its own actionable
  // message instead.
  def e4 =
    TableFormatSetupError.IcebergRest.check(
      restException(422, "Write operation failed: Table metadata file gs://b/x.metadata.json exceeds the maximum allowed size.")
    ) must beSome(
      "Table metadata exceeds the REST catalog's maximum allowed size. Expire old snapshots and compact the table to reduce its metadata size."
    )

  // BigLake's 403 leads with "Not authorized to make this request" -- the REST spec's canonical 403
  // message -- then appends its own permission detail, so it is matched by the same check and triaged
  // as a permission setup error.
  def e5 =
    TableFormatSetupError.IcebergRest.check(
      restException(
        403,
        "Not authorized to make this request. Permission 'biglake.catalogs.get' denied on project 'p' (or it may not exist)."
      )
    ) must beSome("Missing privileges on the catalog, schema or table")

  def e6 =
    TableFormatSetupError.IcebergRest.check(
      restException(404, "Project p is not found. Make sure it references valid GCP project that hasn't been deleted.")
    ) must beSome("Unable to find the given GCP project for the catalog")

  def e7 =
    (TableFormatSetupError.IcebergRest.check(new SnowflakeKeyPairAuthManager.UnparseablePrivateKeyError("bad key", null)) must beSome(
      "Snowflake private key cannot be parsed. Check that it is a PKCS#8 PEM key and that its passphrase is correct"
    )) and
      (TableFormatSetupError.IcebergRest.check(new SnowflakeKeyPairAuthManager.NonRsaPrivateKeyError("EC key")) must beSome(
        "Snowflake private key is not an RSA key"
      ))

  // Snowplow writes the other options, so a missing one is ours to fix, not the customer's
  def e8 =
    TableFormatSetupError.IcebergRest.check(new SnowflakeKeyPairAuthManager.MissingOptionError("missing")) must beNone

  // Unity Catalog's 403 for a missing USE SCHEMA, in Iceberg's error format
  def e9 =
    TableFormatSetupError.IcebergRest.check(
      restException(
        403,
        "Permission denied accessing table: pdp_2927.snowplow.events [ErrorCode: 3001] (request_id: 26b80f50-455b-4b34-a275-8149d72bf6fd)"
      )
    ) must beSome("Missing privileges on the catalog, schema or table")

  // Databricks' 403 for a service principal without the workspace entitlement the REST catalog API
  // needs. It is in Databricks' own JSON format, which Iceberg cannot parse, so the message is the
  // raw body - as the helper builds it here
  def e10 = {
    val body =
      """{"error_code":403,"message":"This API is disabled for users without the databricks-sql-access or workspace-access or workspace-consume entitlements. Contact your administrator for more information. [ReqId: a3aaa2af-8fb1-4f3c-a76e-591240e28f8b]"}"""
    val e = restException(403, body)
    (e.getMessage must beEqualTo(s"Forbidden: $body")) and
      (TableFormatSetupError.IcebergRest.check(e) must beSome(
        "REST catalog API is disabled for the loader's credentials. Check the entitlements granted to the loader's service principal"
      ))
  }

  def e11 = TableFormatSetupError.IcebergRest.check(restException(403, "Something we have never seen")) must beNone

  def e12 =
    TableFormatSetupError.IcebergRest.check(
      new NoSuchNamespaceException("Namespace snowplow_missing not found. (request_id: 1d3c0c1e-6b7f-4b0e-9e0b-0c3b4f1f7c2a)")
    ) must beSome("Unable to find given namespace")

  def e13 =
    TableFormatSetupError.IcebergRest.check(new IcebergWriter.NoVendedCredentialsError("none vended")) must beSome(
      "REST catalog did not provide storage credentials for the table. Check that the catalog allows external engines to access the table's storage"
    )

  // Build the exception through Iceberg's own error handler rather than hand-writing the message, so
  // the test pins the real message format and fails if an Iceberg upgrade changes it. Returns the
  // mapped exception (RESTException, ForbiddenException, ...) for the classifier to inspect.
  private def restException(code: Int, message: String): Throwable =
    try {
      ErrorHandlers.defaultErrorHandler().accept(ErrorResponse.builder().responseCode(code).withMessage(message).build())
      throw new AssertionError(s"expected an exception for code $code")
    } catch {
      case e: RuntimeException => e
    }
}
