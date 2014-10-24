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

import shapeless._
import shapeless.TypeOperators._
import shapeless.UnaryTCConstraint._
import shapeless.Tuples._
import com.twitter.scalding.commons.source.storehaus.cassandra.VersionedCassandraTupleStoreInitializer
import com.twitter.summingbird.batch.{ BatchID, Batcher }

object StorehausCassandraVersionedStore {
  def apply[CKT0 <: Product, CKT0HL <: HList, RKT <: Product, CKT <: Product, CKTHL <: HList, ValueT, RK <: HList, CK <: HList, RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList](
    @transient storeInit: VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult],
    versionsToKeep: Int)(
      implicit inBatcher: Batcher, ord: Ordering[(RKT, CKT0)],
      ev1: HListerAux[CKT0, CKT0HL],
      ev2: PrependAux[CKT0HL, BatchID :: HNil, CKTHL],
      ev3: TuplerAux[CKTHL, CKT],
      ev4: HListerAux[CKT, CKTHL],
      ev5: InitAux[CKTHL, CKT0HL],
      ev6: TuplerAux[CKT0HL, CKT0]): StorehausVersionedBatchStore[(RKT, CKT0), Set[ValueT], (RKT, CKT), Set[ValueT], VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult]] = {

    new StorehausVersionedBatchStore[(RKT, CKT0), Set[ValueT], (RKT, CKT), Set[ValueT], VersionedCassandraTupleStoreInitializer[RKT, CKT, ValueT, RK, CK, RS, CS, MRKResult, MCKResult]](
      storeInit, versionsToKeep, inBatcher)(
      { case (batchID, (k, v)) => ((k._1, (k._2.hlisted :+ batchID).tupled), v) })(
      { case (k, v) => ((k._1, k._2.hlisted.init.tupled), v) }) {
      override def select(b: List[BatchID]) = List(b.last)
    }

  }
}
