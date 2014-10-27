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

import com.twitter.storehaus.cassandra.cql.CASStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.twitter.storehaus.cassandra.cql.CQLCassandraStore
import com.websudos.phantom.CassandraPrimitive

/**
 * Base of Cassandra VersionStores building on CQLCassandraStore
 */
abstract class CassandraVersionStoreFactoryBase[S](cf: StoreColumnFamily) extends VersionStoreFactory[S] {
  // initializer code creating the cassandra schema
  CQLCassandraStore.createColumnFamilyWithToken[Long, Boolean, Long](
    cf, Some(implicitly[CassandraPrimitive[Long]]))
}

/**
 * A simple VersionStoreFactory building on CQLCassandraStore
 */
case class CassandraVersionStoreFactory(cf: StoreColumnFamily) extends CassandraVersionStoreFactoryBase[StorehausVersionStoreT](cf) {
  override def makeStore = (new CQLCassandraStore[Long, Boolean](cf)).
    asInstanceOf[StorehausVersionStoreT]
}
