/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.properties.OozieProperties

import scala.collection.immutable._

/**
  * Config for yarn Actions
  * @param jobTracker Job tracker URL
  * @param nameNode name node url
  */
case class YarnConfig(jobTracker: String, nameNode: String) extends OozieProperties {

  private val jobTrackerName: String = "jobTracker"
  private val nameNodeName: String = "nameNode"

  /**
    * Get the XML for the jobTracker property
    */
  private[scoozie] def jobTrackerXML =
    <job-tracker>{formatProperty(jobTrackerName)}</job-tracker>

  /**
    * Get the XML for the nameNode property
    */
  private[scoozie] def nameNodeXML =
    <name-node>{formatProperty(nameNodeName)}</name-node>

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(formatProperty(jobTrackerName) -> jobTracker, formatProperty(nameNodeName) -> nameNode)
}
