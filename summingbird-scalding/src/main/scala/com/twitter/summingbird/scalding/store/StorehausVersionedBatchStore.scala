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

package com.twitter.summingbird.scalding.store

import cascading.flow.FlowDef
import com.twitter.algebird.monad.Reader
import com.twitter.storehaus.cascading.versioned.VersionedStorehausCascadingInitializer
import com.twitter.scalding.commons.source.storehaus.{ StorehausVersionedMappable, ManagedVersionedStore }
import com.twitter.scalding.{ Dsl, Mode, TDsl, TypedPipe, Hdfs => HdfsMode, TupleSetter }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.scalding.{ Try, FlowProducer, Scalding }

/**
 * An extension of VersionedBatchStoreBase that is based on an underlying storehaus cascading versioned store.
 */
class StorehausVersionedBatchStore[K, V, K2, V2, I <: VersionedStorehausCascadingInitializer[K2, V2]](
    @transient storeInit: I with ManagedVersionedStore,
    val versionsToKeep: Int,
    override val batcher: Batcher)(
        pack: (BatchID, (K, V)) => (K2, V2))(
            unpack: ((K2, V2)) => (K, V))(
                implicit override val ordering: Ordering[K]) extends VersionedBatchStoreBase[K, V]("root_path_unused") {

  /**
   * Uses a VersionedStorehausCascadingInitializer to retrieve
   * the latest existing version living in the batched store
   * and construct its batchID and corresponding FlowProducer from it.
   * The batchID is the EXCLUSIVE upper bound of the last batch.
   */
  override protected def lastBatch(exclusiveUB: BatchID, mode: HdfsMode): Option[(BatchID, FlowProducer[TypedPipe[(K, V)]])] =
    storeInit.lastVersionBefore(batchIDToVersion(exclusiveUB)).map { ver => (versionToBatchID(ver), readVersion(ver)) }

  /**
   * Writes packed results into a new version of the underlying
   * versioned storehaus store representing the batch. The batchID is
   * the INCLUSIVE upper bound of the result data. The version of this
   * batch in the underlying store corresponds to the EXCLUSIVE upper
   * bound (batchID.next).
   */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(
    implicit flowDef: FlowDef, mode: Mode): Unit = {
    import Dsl._
    lastVals.map(pack(batchID, _))
      .toPipe((0, 1))
      .write(new StorehausVersionedMappable[K2, V2, I](storeInit, batchIDToVersion(batchID.next)))
  }

  /**
   * Turns the underlying versioned storehaus store into a typed
   * pipe that is being unpacked.
   */
  override protected def readVersion(v: Long): FlowProducer[TypedPipe[(K, V)]] = Reader {
    (flowMode: (FlowDef, Mode)) =>
      TypedPipe.from(StorehausVersionedMappable[K2, V2, I](storeInit, version = v)).map(unpack)
  }

}
