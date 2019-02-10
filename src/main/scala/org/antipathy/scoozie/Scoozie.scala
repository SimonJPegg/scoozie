package org.antipathy.scoozie

import org.antipathy.scoozie.action._
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.configuration.{Configuration => ActionConfiguration}
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.workflow.Workflow
import java.nio.file.Path
import com.typesafe.config.ConfigFactory

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
    * Methods for testing Oozie workflows
    */
  val Test: api.Test.type = api.Test

  /**
    * Oozie workflow
    *
    * @param name the name of the workflow
    * @param path The path to this workflow
    * @param transitions the actions within the workflow
    * @param configuration configuration for this workflow
    * @param yarnConfig The yarn configuration for this workflow
    * @param credentialsOption optional credentials for this workflow
    */
  def workflow(name: String,
               path: String,
               transitions: Node,
               configuration: ActionConfiguration,
               yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]): Workflow =
    Workflow(name, path, transitions, configuration, yarnConfig)

  /**
    * Oozie coOrdinator definition
    * @param name the CoOrdinator name
    * @param frequency the CoOrdinator frequency
    * @param start the CoOrdinator start time
    * @param end the CoOrdinator end time
    * @param timezone the CoOrdinator time-zone
    * @param workflow the workflow to run
    * @param configuration configuration for the workflow
    */
  def coordinator(name: String,
                  frequency: String,
                  start: String,
                  end: String,
                  timezone: String,
                  workflow: Workflow,
                  configuration: ActionConfiguration): Coordinator =
    Coordinator(name, frequency, start, end, timezone, workflow, configuration)

  /**
    * Build an Oozie workflow (and optional coordinator) from the config file at the specified path
    *
    * @param configPath the path to build the artefacts from
    * @return a GeneratedArtefacts object containing a workflow, optional coordinator and job properties
    */
  def fromConfig(configPath: Path): GeneratedArtefacts = GeneratedArtefacts(ConfigFactory.parseFile(configPath.toFile))

}
