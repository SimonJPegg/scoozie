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
package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.sla._

/**
  * methods for building and adding SLAs to oozie nodes
  */
object SLABuilder {

  /**
    * Add an SLA to an oozie node if the definition exists in the config
    *
    * @param nodeWithConfig The node and its configuration
    * @return The node and its configuration with an SLA attached
    */
  def addSLA(nodeWithConfig: NodeWithConfig): NodeWithConfig = nodeWithConfig.node.action match {
    case _ @(_: Fork | _: Decision | _: End | _: Kill | _: Start | _: Join) => nodeWithConfig
    case _ =>
      buildSLA(nodeWithConfig.config, nodeWithConfig.node.name).map { sla =>
        nodeWithConfig.copy(node = nodeWithConfig.node.withSLA(sla))
      }.getOrElse(nodeWithConfig)
  }

  /**
    * Build and SLA from a configuration object
    * @param config The configuration to build from
    * @param ownerName The name of the SLA's owning object
    * @return an SLA
    */
  def buildSLA(config: Config, ownerName: String): Option[OozieSLA] =
    if (config.hasPath(HoconConstants.sla)) {
      Some(OozieSLA(config.getConfig(HoconConstants.sla), ownerName))
    } else None
}
