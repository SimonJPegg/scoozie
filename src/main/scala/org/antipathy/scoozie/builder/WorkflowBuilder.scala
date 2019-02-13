package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.workflow.Workflow

/**
  * class for building Oozie workflows from Hocon
  */
private[scoozie] object WorkflowBuilder {

  /**
    * Build an oozie workflow from the passed in Config file
    * @param config the configuraion to build from
    * @return an OOzie workflow object
    */
  def build(config: Config): Workflow = Workflow(config)

}
