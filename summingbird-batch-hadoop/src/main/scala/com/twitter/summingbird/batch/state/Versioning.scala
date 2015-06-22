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

/**
 * Abstraction of a simple persistence layer backing the VersioningCheckpointStore
 */
trait Versioning {

  /*
   * basic version management interface
   */

  def mostRecentVersion: Option[Long]
  def succeedVersion(version: Long)
  def deleteVersion(version: Long)
  def failVersion(version: Long) = deleteVersion(version)
  def getAllVersions: List[Long]
  def hasVersion(version: Long) = getAllVersions.contains(version)
  def close(millis: Long): Unit = new UnsupportedOperationException

}