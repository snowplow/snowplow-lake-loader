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

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException

class AuthenticationErrorSpec extends Specification {

  def is = s2"""
  unapply should:
    recognize a known authentication error $e1
    ignore an known authentication error $e2
  """

  def e1 = {
    val exception = new AbfsRestOperationException(
      -1,
      "UNKNOWN",
      "HTTP Error 400; url='https://login.microsoftonline.com/0aa000a0-0000-0000-0000-00a000000000/oauth2/token' AADToken: HTTP connection to https://login.microsoftonline.com/0aa000a0-0000-0000-0000-00a000000000/oauth2/token failed for getting token from AzureAD.; requestId='60ed1e1e-e9c5-4ca8-aed0-ecac531c0e00'; contentType='application/json; charset=utf-8'; response '{\"error\":\"invalid_request\",\"error_description\":\"AADSTS90002: Tenant '0aa000a0-0000-0000-0000-00a000000000' not found. Check to make sure you have the correct tenant ID and are signing into the correct cloud. Check with your subscription administrator, this may happen if there are no active subscriptions for the tenant. Trace ID: 60ed1e1e-e9c5-4ca8-aed0-ecac531c0e00 Correlation ID: 83a3ba04-844f-4b1b-a5bc-b72ba692732e Timestamp: 2024-10-21 08:21:53Z\",\"error_codes\":[90002],\"timestamp\":\"2024-10-21 08:21:53Z\",\"trace_id\":\"60ed1e1e-e9c5-4ca8-aed0-ecac531c0e00\",\"correlation_id\":\"83a3ba04-844f-4b1b-a5bc-b72ba692732e\",\"error_uri\":\"https://login.microsoftonline.com/error?code=90002\"}'",
      new RuntimeException("BOOM")
    )
    val expected =
      "Could not authenticate. Tenant is invalid or does not have an active subscription"
    exception match {
      case AuthenticationError(e) if e === expected => ok
      case _                                        => ko(s"Exception is not recognized [${exception}]")
    }
  }

  def e2 = {
    val exception = new AbfsRestOperationException(
      -1,
      "UNKNOWN",
      "foo AADSTS12345 bar",
      new RuntimeException("BOOM")
    )
    exception match {
      case AuthenticationError(e) => ko(s"Exception should not be recognized as a known authentication error. [$e]")
      case _                      => ok
    }
  }
}
