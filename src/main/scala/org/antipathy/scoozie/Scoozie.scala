package org.antipathy.scoozie

import java.nio.file.Path

import com.typesafe.config.ConfigFactory
import org.antipathy.scoozie.action._
import org.antipathy.scoozie.configuration.{Configuration => ActionConfiguration, _}
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.sla.OozieSLA
import org.antipathy.scoozie.workflow.Workflow

/**
  * Entry class for Scoozie.
  *
  * All required functionality is exposed through this object, however you are not limited to accessing
  * the library via this interface
  */
object Scoozie {

  /**
    * Oozie workflow actions
    */
  val Actions: api.Actions.type = api.Actions

  /**
    * Methods for creating Oozie properties
    */
  val Configuration: api.Configuration.type = api.Configuration

  /**
    * Methods for creating Oozie properties
    */
  val FileSystem: api.FileSystem.type = api.FileSystem

  /**
    * Methods for formatting Oozie workflows
    */
  private[scoozie] val Formatting: api.Formatting.type = api.Formatting

  /**
    * Oozie EL functions
    */
  val Functions: api.Functions.type = api.Functions

  /**
    * Action preparation methods
    */
  val Prepare: api.Prepare.type = api.Prepare

  /**
    * Methods for creating SLAs
    */
  val SLA: api.SLA.type = api.SLA

  /**
    * Methods for testing Oozie workflows
    */
  val Test: api.Test.type = api.Test

  /**
    * Oozie workflow definition
    * @param name the name of the workflow
    * @param path The path to this workflow
    * @param transitions the actions within the workflow
    * @param jobXmlOption optional job.xml path
    * @param credentialsOption optional credentials for this workflow
    * @param configuration configuration for this workflow
    * @param yarnConfig The yarn configuration for this workflow
    * @param slaOption Optional SLA for this workflow
    */
  def workflow(name: String,
               path: String,
               transitions: Node,
               jobXmlOption: Option[String],
               configuration: ActionConfiguration,
               yarnConfig: YarnConfig,
               slaOption: Option[OozieSLA] = None)(implicit credentialsOption: Option[Credentials]): Workflow =
    Workflow(name, path, transitions, jobXmlOption, configuration, yarnConfig, slaOption)

  /**
    * Oozie coOrdinator definition
    * @param name the CoOrdinator name
    * @param path the HDFS path of the coordinator
    * @param frequency the CoOrdinator frequency
    * @param start the CoOrdinator start time
    * @param end the CoOrdinator end time
    * @param timezone the CoOrdinator time-zone
    * @param workflow the workflow to run
    * @param configuration configuration for the workflow
    * @param slaOption Optional SLA for this coordinator
    */
  def coordinator(name: String,
                  path: String,
                  frequency: String,
                  start: String,
                  end: String,
                  timezone: String,
                  workflow: Workflow,
                  configuration: ActionConfiguration,
                  slaOption: Option[OozieSLA] = None): Coordinator =
    Coordinator(name, path, frequency, start, end, timezone, workflow, configuration, slaOption)

  /**
    * Build an Oozie workflow (and optional coordinator) from the config file at the specified path
    *
    * @param configPath the path to build the artefacts from
    * @return a GeneratedArtefacts object containing a workflow, optional coordinator and job properties
    */
  def fromConfig(configPath: Path): GeneratedArtefacts =
    GeneratedArtefacts(ConfigFactory.parseFile(configPath.toFile).resolve())

  private[scoozie] val Null: Null = null
}
