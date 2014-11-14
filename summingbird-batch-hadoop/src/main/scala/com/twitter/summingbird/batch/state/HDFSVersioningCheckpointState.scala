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
package com.twitter.summingbird.batch.state

import com.twitter.algebird.{ ExclusiveUpper, InclusiveLower, Intersection }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory

/**
 * This variant of HDFSState builds on abstract VersionedCheckpontState/Store
 */
object HDFSVersioningCheckpointState {

  case class Config(rootPath: String,
    conf: Configuration,
    startTime: Option[Timestamp],
    numBatches: Long)

  def apply(path: String,
    conf: Configuration = new Configuration,
    startTime: Option[Timestamp] = None,
    numBatches: Long = 1)(implicit b: Batcher): VersioningCheckpointState =
    HDFSVersioningCheckpointState(Config(path, conf, startTime, numBatches))

  def apply(config: Config)(implicit batcher: Batcher): VersioningCheckpointState =
    new VersioningCheckpointState(new HDFSVersioningCheckpointStore(config))
}

class HDFSVersioningCheckpointStore(val config: HDFSVersioningCheckpointState.Config)(implicit batcher: Batcher)
    extends VersioningCheckpointStore(config.startTime, config.numBatches) {

  protected lazy val versionedStore =
    new FileVersionTracking(config.rootPath, FileSystem.get(config.conf))

  def getVersioning(): Versioning = { versionedStore }
}
