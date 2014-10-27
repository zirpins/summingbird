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

import scala.collection.JavaConversions._
import org.specs.Specification
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.Store
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.twitter.storehaus.JMapStore
import com.twitter.util.Future
import com.twitter.concurrent.Spool
import org.apache.hadoop.mapred.JobConf
import com.twitter.scalding.commons.source.storehaus.cassandra.VersionedCassandraStoreInitializer

/**
 * Specification of the VersionedCassandraStoreInitializer
 */
class VersionedCassandraStoreInitializerSpec extends Specification {

  /**
   * An extended JMaPStore implementing IterableStore
   */
  class IterableJMapStore[K, V]() extends JMapStore[K, V]() with IterableStore[K, V] {
    override def getAll: Future[Spool[(K, V)]] =
      IterableStore.iteratorToSpool(
        jstore.filter { t: (K, Option[V]) => t._2.nonEmpty }.
          map { t: (K, Option[V]) => (t._1, t._2.get) }.iterator)
  }

  /**
   * A simple VersionStoreFactory building on IterableJMapStore
   */
  class CassandraMemoryVersionStoreFactory extends VersionStoreFactory[StorehausVersionStoreT] {
    override def makeStore = (new IterableJMapStore[Long, Boolean]()).
      asInstanceOf[StorehausVersionStoreT]
  }

  "StorehausVersionTracking" should {

    "Enable succeeding versions and retrieving them" in {
      val versionTracking = new StorehausVersionTracking(new CassandraMemoryVersionStoreFactory())

      versionTracking.mostRecentVersion must_== None
      versionTracking.getValidVersions.length must_== 0

      versionTracking.succeedVersion(1L);
      versionTracking.mostRecentVersion must_== Some(1L)
      versionTracking.getValidVersions.length must_== 1
      versionTracking.hasVersion(1L) must_== true

      versionTracking.succeedVersion(2L);
      versionTracking.mostRecentVersion must_== Some(2L)
      versionTracking.getValidVersions.length must_== 2
      versionTracking.hasVersion(1L) must_== true
      versionTracking.hasVersion(2L) must_== true

      versionTracking.succeedVersion(3L);
      versionTracking.mostRecentVersion must_== Some(3L)
      versionTracking.getValidVersions.length must_== 3
      versionTracking.hasVersion(1L) must_== true
      versionTracking.hasVersion(2L) must_== true
      versionTracking.hasVersion(3L) must_== true
    }

    "Enable deleting versions" in {
      val versionTracking = new StorehausVersionTracking(new CassandraMemoryVersionStoreFactory())

      versionTracking.succeedVersion(1L);
      versionTracking.succeedVersion(2L);
      versionTracking.succeedVersion(3L);

      versionTracking.deleteVersion(1L);
      versionTracking.getValidVersions.length must_== 2
      versionTracking.mostRecentVersion must_== Some(3L)
      versionTracking.hasVersion(1L) must_== false
      versionTracking.hasVersion(2L) must_== true
      versionTracking.hasVersion(3L) must_== true

      versionTracking.deleteVersion(2L);
      versionTracking.getValidVersions.length must_== 1
      versionTracking.mostRecentVersion must_== Some(3L)
      versionTracking.hasVersion(1L) must_== false
      versionTracking.hasVersion(2L) must_== false
      versionTracking.hasVersion(3L) must_== true

      versionTracking.deleteVersion(3L);
      versionTracking.getValidVersions.length must_== 0
      versionTracking.mostRecentVersion must_== None
      versionTracking.hasVersion(1L) must_== false
      versionTracking.hasVersion(2L) must_== false
      versionTracking.hasVersion(3L) must_== false
    }

  }
}

