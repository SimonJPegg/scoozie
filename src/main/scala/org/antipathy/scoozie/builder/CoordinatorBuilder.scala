package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.coordinator.Coordinator

/**
  * object for building coordinators from hocon
  */
object CoordinatorBuilder {

  def build(config: Config): Coordinator = {

    val coordinatorConfig = config.getConfig(HoconConstants.coordinator)
    val workflow = WorkflowBuilder.build(config)
    val name = coordinatorConfig.getString(HoconConstants.name)
    val frequency = coordinatorConfig.getString(HoconConstants.frequency)
    val start = coordinatorConfig.getString(HoconConstants.start)
    val end = coordinatorConfig.getString(HoconConstants.end)
    val timezone = coordinatorConfig.getString(HoconConstants.timezone)
    val configuration = ConfigurationBuilder.buildConfiguration(coordinatorConfig)

    Coordinator(name, frequency, start, end, timezone, workflow, configuration)
  }
}
