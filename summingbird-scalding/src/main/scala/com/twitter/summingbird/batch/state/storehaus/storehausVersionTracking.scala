/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.summingbird.batch.state.storehaus

import org.slf4j.LoggerFactory
import com.twitter.storehaus.{ Store, IterableStore }
import com.twitter.concurrent.Spool
import com.twitter.util.{ Await, Closable }
import com.twitter.summingbird.batch.state.Versioning

/**
 * We use a simple type of storehaus store for keeping version data.
 * This factory allows to use any implementation of StorehausVersionStore.
 */
trait VersionStoreFactory[S] {
  def makeStore(): S
  def createSchema(): Unit
}

/**
 * Simple version tracking based on a basic storehaus Store
 */
class StorehausVersionTracking(factory: VersionStoreFactory[StorehausVersionStoreT])
    extends StorehausVersionTrackingBase[StorehausVersionStoreT] {

  private lazy val store: StorehausVersionStoreT = factory.makeStore

  override protected def getStore = store

  override def succeedVersion(version: Long): Unit = {
    logger.info("Validating version " + version.toString)
    getStore().put((version, Some(true)))
  }

  override def deleteVersion(version: Long): Unit = {
    logger.info("Invalidating version " + version.toString)
    getStore().put((version, Some(false)))
  }

}

/**
 * Represents basic version tracking building on storehaus. Some operations
 * may vary with concrete store types.
 *
 * Similar to {@link com.twitter.summingbird.batch.state.FileVersionTracking}
 */
abstract class StorehausVersionTrackingBase[S <: IterableStore[Long, Boolean] with Closable] extends Versioning {

  protected val logger = LoggerFactory.getLogger(classOf[StorehausVersionTrackingBase[S]])

  protected def logVersion(v: Long) = logger.trace("Version in store : " + v.toString)

  // get concrete store
  protected def getStore(): S

  // eagerly read versions
  private def getStoreVersions: Seq[(Long, Boolean)] =
    Await.result[Seq[(Long, Boolean)]](
      Await.result[Spool[(Long, Boolean)]](
        getStore getAll) toSeq)

  // filter valid versions
  def getValidVersions(): Seq[Long] =
    getStoreVersions.filter { x => x._2 }
      .map { y => logVersion(y._1); y._1 }
      .sorted
      .reverse

  override def getAllVersions = getValidVersions.toList

  override def hasVersion(version: Long): Boolean = getValidVersions().contains(version)

  override def mostRecentVersion: Option[Long] = getValidVersions.headOption

  override def close(): Unit = getStore close

}

