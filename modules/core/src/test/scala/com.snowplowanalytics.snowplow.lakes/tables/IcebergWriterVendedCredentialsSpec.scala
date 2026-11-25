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

import org.apache.iceberg.inmemory.InMemoryFileIO
import org.apache.iceberg.io.{StorageCredential, SupportsStorageCredentials}
import org.specs2.Specification

import java.net.URI
import java.util.{List => JList}
import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.lakes.Config

class IcebergWriterVendedCredentialsSpec extends Specification {

  def is = s2"""
  IcebergWriter.noCredentialsVended should:
    Be false when the FileIO holds storage credentials $e1
    Be true when nothing was vended $e2
    Be false for a FileIO that cannot hold storage credentials $e3

  IcebergWriter.requestsVendedCredentials should:
    Be true when the delegation header asks for vended credentials $e4
    Be true when vended credentials are one of several delegation mechanisms $e5
    Be false when the delegation header is absent $e6
    Be false when the delegation header asks for something else $e7
    Be false for a catalog that is not a REST catalog $e8
    Match the header name case insensitively $e9
    Be false when the option's header prefix is not exactly "header.", because Iceberg would not send it $e10
  """

  def e1 =
    IcebergWriter.noCredentialsVended(
      new StubFileIO(
        List(StorageCredential.create("abfss://c@a.dfs.core.windows.net/t", Map("adls.sas-token.a" -> "sas").asJava))
      )
    ) must beFalse

  /** What Unity Catalog returns when the loader may not reach the table's storage */
  def e2 =
    IcebergWriter.noCredentialsVended(new StubFileIO(Nil)) must beTrue

  def e3 =
    IcebergWriter.noCredentialsVended(new InMemoryFileIO) must beFalse

  def e4 =
    IcebergWriter.requestsVendedCredentials(rest(Map("header.X-Iceberg-Access-Delegation" -> "vended-credentials"))) must beTrue

  def e5 =
    IcebergWriter.requestsVendedCredentials(
      rest(Map("header.X-Iceberg-Access-Delegation" -> "remote-signing, vended-credentials"))
    ) must beTrue

  def e6 =
    IcebergWriter.requestsVendedCredentials(rest(Map("credential" -> "id:secret"))) must beFalse

  def e7 =
    IcebergWriter.requestsVendedCredentials(rest(Map("header.X-Iceberg-Access-Delegation" -> "remote-signing"))) must beFalse

  def e8 =
    IcebergWriter.requestsVendedCredentials(
      Config.IcebergCatalog.Glue(Map("header.X-Iceberg-Access-Delegation" -> "vended-credentials"))
    ) must beFalse

  def e9 =
    IcebergWriter.requestsVendedCredentials(rest(Map("header.x-iceberg-access-delegation" -> "vended-credentials"))) must beTrue

  def e10 =
    IcebergWriter.requestsVendedCredentials(rest(Map("Header.X-Iceberg-Access-Delegation" -> "vended-credentials"))) must beFalse

  private def rest(options: Map[String, String]): Config.IcebergCatalog =
    Config.IcebergCatalog.Rest(URI.create("https://example.com/api/2.1/unity-catalog/iceberg-rest"), "catalog", options)

  private class StubFileIO(creds: List[StorageCredential]) extends InMemoryFileIO with SupportsStorageCredentials {
    override def credentials(): JList[StorageCredential]           = creds.asJava
    override def setCredentials(c: JList[StorageCredential]): Unit = ()
  }
}
