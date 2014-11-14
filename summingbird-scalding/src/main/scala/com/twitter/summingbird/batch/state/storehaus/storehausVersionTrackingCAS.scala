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

import com.twitter.util.Await
import org.slf4j.LoggerFactory
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.cassandra.cql.CASStore
import com.twitter.storehaus.Store
import com.twitter.util.Closable

/**
 * Version tracking with some concurrency control based on a CASStore
 *
 * TODO: enable exception handling for unsuccessful CAS operations
 */
class StorehausVersionTrackingCAS(factory: VersionStoreFactory[StorehausVersionStoreCasT])
    extends StorehausVersionTrackingBase[StorehausVersionStoreCasT] {

  private lazy val store: StorehausVersionStoreCasT = factory.makeStore

  protected def getStore = store

  override def succeedVersion(version: Long): Unit = {
    logger.debug("Validating version " + version.toString)
    store.get(version) onSuccess { reply =>
      reply match {
        case None => store.cas(None, (version, true))
        case Some(result) => if (!result._1) store.cas(Some(result._2), (version, true))
      }
    }
  }

  override def deleteVersion(version: Long): Unit = {
    logger.debug("Invalidating version " + version.toString)
    store.get(version) onSuccess { reply =>
      reply match {
        case None => store.cas(None, (version, false))
        case Some(result) => if (result._1) store.cas(Some(result._2), (version, false))
      }
    }
  }

}
