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
import shapeless.Tuples._
//import shapeless.TypeOperators._
//import shapeless.UnaryTCConstraint._

import com.websudos.phantom.CassandraPrimitive
import StorehausCassandraVersionedStore.BatchIDAsCassandraPrimitive

/*
 * Two versions of implementing storehaus batch stores building on cassandra tupple stores.
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
    def cls : Class[_] = classOf[BatchID]
    def fromRow(row : Row, name : String) : Option[BatchID] = if (row.isNull(name)) None else ScalaTry(new BatchID(row.getLong(name))).toOption
    override def toCType(v : BatchID) : AnyRef = v.id.asInstanceOf[AnyRef]
    override def fromCType(c : AnyRef) : BatchID = new BatchID(c.asInstanceOf[Long])
  }

  /**
   * Factory variant creating storehaus batch stores building on cassandra tupple stores
   *
   * Problem here: transitivity of hlist operators is not respected by Scala, which
   * causes jobs to throw during flowDef serialization.
   *
   */
  def apply[CKT0 <: Product, CKT0HL <: HList, RKT <: Product, CKT <: Product, CKTHL <: HList, ValueT, RK <: HList, CK <: HList, RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList](
    @transient storeInit : VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult],
    versionsToKeep : Int)(
      implicit inBatcher : Batcher,
      ord : Ordering[(RKT, CKT0)],
      @transient scvsOEv1 : HListerAux[CKT0, CKT0HL],
      @transient scvsOEv2 : PrependAux[CKT0HL, BatchID :: HNil, CKTHL],
      @transient scvsOEv3 : TuplerAux[CKTHL, CKT],
      @transient scvsOEv4 : HListerAux[CKT, CKTHL],
      @transient scvsOEv5 : InitAux[CKTHL, CKT0HL],
      @transient scvsOEv6 : TuplerAux[CKT0HL, CKT0]) =
    new StorehausVersionedBatchStore[(RKT, CKT0), Set[ValueT], (RKT, CKT), Set[ValueT], VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult]](
      storeInit, versionsToKeep, inBatcher)(
      { case (batchID, (k, v)) => ((k._1, (k._2.hlisted :+ batchID.next).tupled), v) })(
      { case (k, v) => ((k._1, k._2.hlisted.init.tupled), v) })
}

/**
 * An extended storehaus batch store building on a cassandra tupple store.
 *
 * Pack/Unpack is implemented based on hlist operations. Scala 2.10 ignores
 * their transient annotation and throws during flowDef serialization.
 *
 */
class StorehausCassandraVersionedStore[CKT0 <: Product, CKT0HL <: HList, RKT <: Product, CKT <: Product, CKTHL <: HList, ValueT, RK <: HList, CK <: HList, RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList](
  @transient val storeInit : VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult],
  val versionsToKeep : Int)(
    implicit inBatcher : Batcher,
    ord : Ordering[(RKT, CKT0)],
    @transient val scvsCEv1 : HListerAux[CKT0, CKT0HL],
    @transient val scvsCEv2 : PrependAux[CKT0HL, BatchID :: HNil, CKTHL],
    @transient val scvsCEv3 : TuplerAux[CKTHL, CKT],
    @transient val scvsCEv4 : HListerAux[CKT, CKTHL],
    @transient val scvsCEv5 : InitAux[CKTHL, CKT0HL],
    @transient val scvsCEv6 : TuplerAux[CKT0HL, CKT0])
  extends StorehausVersionedBatchStore[(RKT, CKT0), Set[ValueT], (RKT, CKT), Set[ValueT], VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult]](
    storeInit,
    versionsToKeep,
    inBatcher)(
    { case (batchID, (k, v)) => ((k._1, (k._2.hlisted :+ batchID.next).tupled), v) })(
    { case (k, v) => ((k._1, k._2.hlisted.init.tupled), v) }) 
