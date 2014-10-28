/*
 Copyright 2013 Twitter, Inc.

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

import com.twitter.summingbird.batch.state.Versioning
import com.twitter.summingbird.batch.state.VersioningCheckpointStoreFactory
import com.twitter.summingbird.batch.state.VersioningCheckpointStore
import com.twitter.summingbird.batch.state.VersioningCheckpointState
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.batch.state.VersioningCheckpointStore
import com.twitter.summingbird.batch.state.VersioningCheckpointStoreFactory
import com.twitter.summingbird.batch.state.Versioning
import com.twitter.summingbird.batch.state.VersioningCheckpointState

object StorehausVersioningCheckpointState {

  case class Config(
    init: CassandraVersionStoreFactory,
    startTime: Option[Timestamp],
    numBatches: Long)

  def apply(
    init: CassandraVersionStoreFactory,
    startTime: Option[Timestamp] = None,
    numBatches: Long = 1)(implicit b: Batcher): VersioningCheckpointState =
    StorehausVersioningCheckpointState(Config(init, startTime, numBatches))

  def apply(config: Config)(implicit batcher: Batcher): VersioningCheckpointState =
    new VersioningCheckpointState(new StorehausCheckpointStoreFactory(config))

}

class StorehausCheckpointStoreFactory(config: StorehausVersioningCheckpointState.Config)(implicit batcher: Batcher)
    extends VersioningCheckpointStoreFactory {
  def getVersioningCheckpointStore = new StorehausVersioningCheckpointStore(config)
}

class StorehausVersioningCheckpointStore(val config: StorehausVersioningCheckpointState.Config)(implicit batcher: Batcher)
    extends VersioningCheckpointStore(config.startTime, config.numBatches) {

  protected lazy val versionedStore =
    new StorehausVersionTracking(config.init)

  def getVersioning(): Versioning = versionedStore
}
