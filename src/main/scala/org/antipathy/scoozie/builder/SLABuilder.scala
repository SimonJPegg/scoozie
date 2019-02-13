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
