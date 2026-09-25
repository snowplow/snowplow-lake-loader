/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.tables

import org.specs2.Specification

class WriterSpec extends Specification {

  def is = s2"""
  Writer.describeProperties should:
    Sort by key and quote values $e1
    Describe no properties as none $e2
  """

  /** The value with a comma in it is the case the quoting exists for. */
  def e1 =
    Writer.describeProperties(Map("b" -> "2", "a" -> "1,2", "c" -> "3")) must
      beEqualTo("a=\"1,2\", b=\"2\", c=\"3\"")

  def e2 =
    Writer.describeProperties(Map.empty) must beEqualTo("none")
}
