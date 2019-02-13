package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.coordinator.Coordinator

/**
  * object for building coordinators from hocon
  */
object CoordinatorBuilder {

  /**
    * Build a coordinator from the passed in config
    * @param config the config to build from
    * @return a coordinator
    */
  def build(config: Config): Coordinator = Coordinator(config)
}
