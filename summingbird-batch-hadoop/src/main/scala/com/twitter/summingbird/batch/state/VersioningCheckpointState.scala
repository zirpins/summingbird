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

package com.twitter.summingbird.batch.state

import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.batch.Batcher
import com.twitter.algebird.InclusiveLower
import com.twitter.algebird.Intersection
import com.twitter.algebird.ExclusiveUpper
import com.twitter.summingbird.batch.Timestamp
import org.slf4j.LoggerFactory

/**
 * Checkpoint state implementation based on a class of simple version-keeping stores.
 */
class VersioningCheckpointState(val store: VersioningCheckpointStore)
    extends CheckpointState[Iterable[BatchID]] {
  override def checkpointStore = { store }
}

/**
 * Abstract parts of a CheckpointStore that is backed by a simple version store.
 *
 * Concrete version stores might be based on HDFS, storehaus or other persitence layers.
 */
abstract class VersioningCheckpointStore(startTime: Option[Timestamp], numBatches: Long)(implicit val batcher: Batcher)
    extends CheckpointStore[Iterable[BatchID]] {

  private val logger = LoggerFactory.getLogger(classOf[VersioningCheckpointStore])

  @transient def version(b: BatchID) = batcher.earliestTimeOf(b).milliSinceEpoch

  // how to set up a concrete underlying version store
  def getVersioning(): Versioning

  // close the underlying persistence layer
  def close(): Unit = { getVersioning.close }

  val startBatch: InclusiveLower[BatchID] =
    startTime.map(batcher.batchOf(_))
      .orElse {
        val mostRecentB = getVersioning.mostRecentVersion
          .map(t => batcher.batchOf(Timestamp(t)).next)
        logger.info("Most recent batch not on disk: " + mostRecentB.toString)
        mostRecentB
      }.map(InclusiveLower(_)).getOrElse {
        sys.error {
          "You must provide startTime in config " +
            "at least for the first run!"
        }
      }

  val endBatch: ExclusiveUpper[BatchID] = ExclusiveUpper(startBatch.lower + numBatches)

  override def checkpointBatchStart(intersection: Intersection[InclusiveLower, ExclusiveUpper, Timestamp]): Iterable[BatchID] =
    BatchID.toIterable(batcher.batchesCoveredBy(intersection))

  override def checkpointSuccessfulRun(runningBatches: Iterable[BatchID]) =
    runningBatches.foreach { b => getVersioning.succeedVersion(version(b)) }

  override def checkpointFailure(runningBatches: Iterable[BatchID], err: Throwable) =
    runningBatches.foreach { b => getVersioning.deleteVersion(version(b)) }

  override def checkpointPlanFailure(err: Throwable) = throw err

}
