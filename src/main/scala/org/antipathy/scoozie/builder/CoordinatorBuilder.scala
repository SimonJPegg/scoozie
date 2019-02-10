package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.coordinator.Coordinator

/**
  * object for building coordinators from hocon
  */
object CoordinatorBuilder {

  def build(config: Config): Coordinator = {

    val coordinatorConfig = config.getConfig("coordinator")
    val workflow = WorkflowBuilder.build(config)
    val name = coordinatorConfig.getString("name")
    val frequency = coordinatorConfig.getString("frequency")
    val start = coordinatorConfig.getString("start")
    val end = coordinatorConfig.getString("end")
    val timezone = coordinatorConfig.getString("timezone")
    val configuration = ConfigurationBuilder.buildConfiguration(coordinatorConfig)

    Coordinator(name, frequency, start, end, timezone, workflow, configuration)
  }
}
