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

class DeltaWriterTablePropertiesSpec extends Specification {

  def is = s2"""
  DeltaWriter.tableProperties should
    remove a partition column from the stats list $removes
    match entries that carry spaces around them $trims
    match backticked entries $backticks
    match names case-insensitively $caseInsensitive
    leave the configured text untouched when it removes nothing $untouched
    leave the properties unchanged when the stats list is absent $absent
    keep every other property $otherProperties
  """

  private val stats = "delta.dataSkippingStatsColumns"

  private val partitioned = Set("load_tstamp_date", "event_name")

  private def statsAfter(list: String, partitionColumns: Set[String] = partitioned): Option[String] =
    DeltaWriter.tableProperties(Map(stats -> list), partitionColumns).get(stats)

  def removes =
    statsAfter("load_tstamp,event_name,app_id") must beSome("load_tstamp,app_id")

  def trims =
    statsAfter("load_tstamp, event_name ,app_id") must beSome("load_tstamp,app_id")

  def backticks =
    statsAfter("load_tstamp,`event_name`,app_id") must beSome("load_tstamp,app_id")

  def caseInsensitive =
    statsAfter("load_tstamp,EVENT_NAME,app_id") must beSome("load_tstamp,app_id")

  def untouched = {
    val list = "load_tstamp, `app_id` ,collector_tstamp"
    (statsAfter(list) must beSome(list)) and (statsAfter(list, Set.empty) must beSome(list))
  }

  def absent = {
    val configured = Map("delta.checkpointInterval" -> "50")
    DeltaWriter.tableProperties(configured, partitioned) must_== configured
  }

  def otherProperties = {
    val configured = Map("delta.checkpointInterval" -> "50", stats -> "load_tstamp,event_name")
    DeltaWriter.tableProperties(configured, partitioned) must_== Map("delta.checkpointInterval" -> "50", stats -> "load_tstamp")
  }
}
