package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.workflow.Workflow
import scala.collection.JavaConverters._
import scala.collection.immutable._

/**
  * class for building Oozie workflows from Hocon
  */
private[scoozie] object WorkflowBuilder {

  /**
    * Build an oozie workflow from the passed in Config file
    * @param config the configuraion to build from
    * @return an OOzie workflow object
    */
  def build(config: Config): Workflow = {

    implicit val credentials: Option[Credentials] =
      ConfigurationBuilder.buildCredentials(config.getConfig("workflow"))

    val name = config.getString("workflow.name")
    val path = config.getString("workflow.path")
    val configuration: Configuration =
      ConfigurationBuilder.buildConfiguration(config.getConfig("workflow"))

    val yarnConfig =
      YarnConfig(config.getString("workflow.yarn-config.name-node"),
                 config.getString("workflow.yarn-config.job-tracker"))

    val transitions = TransitionBuilder.build(Seq(config.getConfigList("workflow.transitions").asScala: _*), yarnConfig)

    val jobXml = if (config.hasPath("workflow.job-xml")) {
      Some(config.getString("workflow.job-xml"))
    } else None

    Workflow(name, path, transitions, jobXml, configuration, yarnConfig)
  }

}
