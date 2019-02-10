package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.action.Node

/**
  * Internal class for storing nodes with their config
  */
private[builder] case class NodeWithConfig(node: Node, config: Config)
