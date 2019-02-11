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
      ConfigurationBuilder.buildCredentials(config.getConfig(HoconConstants.workflow))

    val name = config.getString(s"${HoconConstants.workflow}.${HoconConstants.name}")
    val path = config.getString(s"${HoconConstants.workflow}.${HoconConstants.path}")
    val configuration: Configuration =
      ConfigurationBuilder.buildConfiguration(config.getConfig(HoconConstants.workflow))

    val yarnConfig =
      YarnConfig(
        config.getString(s"${HoconConstants.workflow}.${HoconConstants.yarnConfig}.${HoconConstants.nameNode}"),
        config.getString(s"${HoconConstants.workflow}.${HoconConstants.yarnConfig}.${HoconConstants.jobTracker}")
      )

    val transitions = TransitionBuilder.build(
      Seq(config.getConfigList(s"${HoconConstants.workflow}.${HoconConstants.transitions}").asScala: _*),
      yarnConfig
    )

    val jobXml = if (config.hasPath(s"${HoconConstants.workflow}.${HoconConstants.jobXml}")) {
      Some(config.getString(s"${HoconConstants.workflow}.${HoconConstants.jobXml}"))
    } else None

    Workflow(name, path, transitions, jobXml, configuration, yarnConfig)
  }

}
