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

import org.apache.iceberg.{TableMetadata, TableOperations}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.encryption.EncryptionManager
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.{FileIO, LocationProvider}

import java.util.concurrent.atomic.AtomicReference

import com.snowplowanalytics.snowplow.lakes.tables.{IcebergWriter, Writer}

/**
 * A `HadoopCatalog` whose next window commit is applied and then reported as an error, as a REST
 * catalog does when it applies a commit and the response is lost.
 *
 * Spark instantiates the catalog from its class name, so what to throw is set on the companion
 * object. Only a commit adding a snapshot tagged with a window other than the startup commit's is
 * failed, and only once per `failNextWindowCommit`.
 */
class AmbiguousCommitCatalog extends HadoopCatalog {
  override protected def newTableOps(identifier: TableIdentifier): TableOperations =
    new AmbiguousCommitCatalog.Ops(super.newTableOps(identifier))
}

object AmbiguousCommitCatalog {

  private val pending = new AtomicReference[Option[() => Throwable]](None)

  def failNextWindowCommit(error: () => Throwable): Unit = pending.set(Some(error))

  def clear(): Unit = pending.set(None)

  private class Ops(delegate: TableOperations) extends TableOperations {
    def current: TableMetadata                     = delegate.current
    def refresh: TableMetadata                     = delegate.refresh
    def io: FileIO                                 = delegate.io
    override def encryption: EncryptionManager     = delegate.encryption
    def metadataFileLocation(name: String): String = delegate.metadataFileLocation(name)
    def locationProvider: LocationProvider         = delegate.locationProvider
    override def temp(m: TableMetadata)            = delegate.temp(m)
    override def newSnapshotId: Long               = delegate.newSnapshotId
    override def requireStrictCleanup: Boolean     = delegate.requireStrictCleanup

    def commit(base: TableMetadata, metadata: TableMetadata): Unit = {
      delegate.commit(base, metadata)
      if (addsWindowSnapshot(base, metadata))
        pending.getAndSet(None).foreach(error => throw error())
    }

    private def addsWindowSnapshot(base: TableMetadata, metadata: TableMetadata): Boolean =
      Option(metadata.currentSnapshot).exists { snapshot =>
        val isNew  = Option(base).flatMap(b => Option(b.currentSnapshot)).forall(_.snapshotId != snapshot.snapshotId)
        val window = Option(snapshot.summary.get(IcebergWriter.windowIdProperty)).flatMap(Writer.WindowId.parse).map(_.window)
        isNew && window.exists(_ != Writer.WindowId.startupWindow)
      }
  }
}
