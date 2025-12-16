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

import org.apache.iceberg.exceptions.{ForbiddenException => IcebergForbiddenException, NoSuchIcebergTableException, NotFoundException => IcebergNotFoundException, RESTException, NoSuchNamespaceException, BadRequestException, NotAuthorizedException}

import org.apache.iceberg.rest.auth.OAuth2Properties

import org.apache.spark.sql.delta.DeltaAnalysisException

import java.net.UnknownHostException

object TableFormatSetupError {

  // Check if given exception is specific to iceberg format
  def check(targetType: String): PartialFunction[Throwable, String] =
    targetType match {
      case "delta" => Delta.check.unlift
      case "iceberg-glue" => IcebergGlue.check.unlift
      case "iceberg-rest" => IcebergRest.check.unlift
      case _ => PartialFunction.empty
    }

  object Delta {
    def check: Throwable => Option[String] = {
      case e: DeltaAnalysisException if e.errorClass == Some("DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION") =>
        Some("Destination not empty and not a Delta table")
      case _ => None
    }
  }

  object IcebergGlue {
    def check: Throwable => Option[String] = {
      case _: NoSuchIcebergTableException =>
        // Table exists but not in Iceberg format
        Some("Target table is not an Iceberg table")
      case e: IcebergNotFoundException =>
        // Glue catalog does not exist
        Some(e.getMessage)
      case e: IcebergForbiddenException =>
          // No permission to create a table in Glue catalog
          Some(e.getMessage)
      case _ => None
    }
  }

  object IcebergRest {
    def check: Throwable => Option[String] = {
      case e: IcebergForbiddenException =>
        if (checkS3RestCatalogPermissionError(e))
          Some("IAM role of the REST catalog is missing permissions")
        else if (checkRestCatalogRolePermissionError(e))
          Some("REST catalog role is missing permissions")
        else
          None
      case e: NoSuchNamespaceException =>
        Some(e.getMessage)
      case e: BadRequestException =>
        extractOauthErrorType(e).map(oauthErrorMessage)
      case e: NotAuthorizedException =>
        extractOauthErrorType(e).map(oauthErrorMessage)
      case e: RESTException if e.getMessage.contains("Unable to process") =>
        if (e.getMessage.contains("Unable to find warehouse"))
          Some("Unable to find given catalog")
        else if (e.getMessage.contains("Service: S3, Status Code: 301"))
          Some("REST catalog returned an error. Check your REST catalog configuration. A possible cause is invalid S3 bucket region for this catalog")
        else
          Some("REST catalog returned an error. Check your REST catalog configuration")
      case e: RESTException if Option(e.getCause).exists(_.isInstanceOf[UnknownHostException]) =>
        Some("REST catalog URI isn't reachable")
      case _ => None
    }

    private def checkS3RestCatalogPermissionError(exception: IcebergForbiddenException): Boolean = {
      val badRequestPattern = """.*is not authorized to perform.*no identity-based policy allows.*""".r
      badRequestPattern.matches(exception.getMessage)
    }

    private def checkRestCatalogRolePermissionError(exception: IcebergForbiddenException): Boolean = {
      val badRequestPattern = """.*Principal.*with activated PrincipalRoles.*is not authorized for op.*""".r
      badRequestPattern.matches(exception.getMessage)
    }

    private def extractOauthErrorType(exception: RESTException): Option[String] = {
      val errorTypes = List(
        OAuth2Properties.INVALID_REQUEST_ERROR,
        OAuth2Properties.INVALID_CLIENT_ERROR,
        OAuth2Properties.INVALID_GRANT_ERROR,
        OAuth2Properties.UNAUTHORIZED_CLIENT_ERROR,
        OAuth2Properties.UNSUPPORTED_GRANT_TYPE_ERROR,
        OAuth2Properties.INVALID_SCOPE_ERROR
      )
      val badRequestPattern = """.*Malformed request: (\w+): .*""".r
      val notAuthorizedPattern = """.*Not authorized: (\w+): .*""".r

      val errorType = exception.getMessage match {
        case badRequestPattern(errorType) => Some(errorType)
        case notAuthorizedPattern(errorType) => Some(errorType)
        case _ => None
      }
      errorType.filter(errorTypes.contains(_))
    }

    private def oauthErrorMessage(errorType: String): String =
      s"OAuth error from REST Iceberg catalog: $errorType"
  }
}
