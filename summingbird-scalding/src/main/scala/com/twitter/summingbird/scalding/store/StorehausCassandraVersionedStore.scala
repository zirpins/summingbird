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

import scala.util.{ Try => ScalaTry }

import com.datastax.driver.core.Row
import com.twitter.scalding.commons.source.storehaus.cassandra.VersionedCassandraTupleStoreInitializer
import com.twitter.summingbird.batch.{ BatchID, Batcher }

import com.twitter.bijection.{ Bufferable, Codec, Injection }

import shapeless._
import shapeless.ops.tuple._
import syntax.std.tuple._

import com.websudos.phantom.CassandraPrimitive
import StorehausCassandraVersionedStore.BatchIDAsCassandraPrimitive

/*
 * Two versions of implementing storehaus batch stores building on cassandra tuple stores.
 * 
 * Both suffer from (non critical) serialization problems due to neglected transient annotations.
 * 
 */

object StorehausCassandraVersionedStore {

  /**
   * CassandraPrimitive for batchID
   */
  implicit object BatchIDAsCassandraPrimitive extends CassandraPrimitive[BatchID] {
    val cassandraType = "bigint"
    def cls: Class[_] = classOf[BatchID]
    def fromRow(row: Row, name: String): Option[BatchID] = if (row.isNull(name)) None else ScalaTry(new BatchID(row.getLong(name))).toOption
    override def toCType(v: BatchID): AnyRef = v.id.asInstanceOf[AnyRef]
    override def fromCType(c: AnyRef): BatchID = new BatchID(c.asInstanceOf[Long])
  }

  /**
   * Factory variant creating storehaus batch stores building on cassandra tupple stores
   *
   * Problem here: transitivity of hlist operators is not respected by Scala, which
   * causes jobs to throw during flowDef serialization.
   *
   */
  def apply[CKT0 <: Product, RKT <: Product, CKT <: Product, ValueT, RK <: HList, CK <: HList, RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList](
    storeInit: VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult],
    versionsToKeep: Int)(
      implicit inBatcher: Batcher,
      ord: Ordering[(RKT, CKT0)],
      @transient scvsOEv2: Prepend.Aux[CKT0, Tuple1[BatchID], CKT],
      @transient scvsOEv5: Init.Aux[CKT, CKT0]) =
    new StorehausVersionedBatchStore[(RKT, CKT0), Set[ValueT], (RKT, CKT), Set[ValueT], VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult]](
      storeInit, versionsToKeep, inBatcher)(
      { case (batchID, (k, v)) => ((k._1, k._2 :+ batchID), v) })(
      { case (k, v) => ((k._1, k._2.init), v) })
}

/**
 * An extended storehaus batch store building on a cassandra tupple store.
 *
 * Pack/Unpack is implemented based on hlist operations. Scala 2.10 ignores
 * their transient annotation and throws during flowDef serialization.
 *
 */
class StorehausCassandraVersionedStore[CKT0 <: Product, RKT <: Product, CKT <: Product, ValueT, RK <: HList, CK <: HList, RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList](
  storeInit: VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult],
  versionsToKeep: Int)(
    implicit inBatcher: Batcher,
    ord: Ordering[(RKT, CKT0)],
    @transient val scvsCEv2: Prepend.Aux[CKT0, Tuple1[BatchID], CKT],
    @transient val scvsCEv5: Init.Aux[CKT, CKT0])
    extends StorehausVersionedBatchStore[(RKT, CKT0), Set[ValueT], (RKT, CKT), Set[ValueT], VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult]](
      storeInit,
      versionsToKeep,
      inBatcher)(
      { case (batchID, (k, v)) => ((k._1, k._2 :+ batchID), v) })(
      { case (k, v) => ((k._1, k._2.init), v) })
