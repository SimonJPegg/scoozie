package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration._
import scala.xml.Elem
import org.antipathy.scoozie.Node
import org.antipathy.scoozie.configuration.Credentials
import scala.collection.immutable._

/**
  *
  * Oozie Hive action definition
  * @param name the name of the action
  * @param scriptName the name of the script
  * @param scriptLocation the path to the script
  * @param commandLineArgs command line arguments for script
  * @param envVars environment variables for the script
  * @param files files to include with the script
  * @param captureOutput capture output from this action
  * @param config Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
final class ShellAction(override val name: String,
                        scriptName: String,
                        scriptLocation: String,
                        commandLineArgs: Seq[String],
                        envVars: Seq[String],
                        files: Seq[String],
                        captureOutput: Boolean,
                        config: YarnConfig,
                        prepareOption: Option[Prepare] = None)
    extends Action {

  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val commandLineArgsProperties =
    buildSequenceProperties(name, "commandLineArgs", commandLineArgs)
  private val envVarsProperties =
    buildSequenceProperties(name, "envVars", envVars)
  private val filesProperties = buildSequenceProperties(name, "files", files)
  private val prepareOptionAndProps =
    prepareOption.map(_.withActionProperties(name))
  private val prepareProperties =
    prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)
  private val mappedConfigAndProperties =
    config.configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties._1

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    config.properties ++ Map(
      scriptNameProperty -> scriptName,
      scriptLocationProperty -> scriptLocation
    ) ++ prepareProperties ++
    commandLineArgsProperties ++
    envVarsProperties ++
    mappedConfigAndProperties._2

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:shell-action:0.1")

  /**
    * The XML for this node
    */
  override def toXML: Elem = {
    val fileVal = s"$scriptLocationProperty#$scriptNameProperty"
    <shell xmlns={xmlns.orNull}>
      {config.jobTrackerXML}
      {config.nameNodeXML}
      {if (prepareOptionMapped.isDefined) {
        prepareOptionMapped.map(_.toXML)
        }
      }
      {if (mappedConfig.configProperties.nonEmpty) {
          mappedConfig.toXML
        }
      }
      <exec>{scriptNameProperty}</exec>
      {commandLineArgsProperties.keys.map(Argument(_).toXML)}
      {envVarsProperties.keys.map(EnvVar(_).toXML)}
      {filesProperties.keys.map(File(_).toXML)}
      <file>{fileVal}</file>
      { if (captureOutput) {
          <capture-output />
        }
      }
    </shell>
  }
}

object ShellAction {

  def apply(
      name: String,
      scriptName: String,
      scriptLocation: String,
      commandLineArgs: Seq[String],
      envVars: Seq[String],
      files: Seq[String],
      captureOutput: Boolean,
      config: YarnConfig,
      prepareOption: Option[Prepare] = None
  )(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new ShellAction(name,
                      scriptName,
                      scriptLocation,
                      commandLineArgs,
                      envVars,
                      files,
                      captureOutput,
                      config,
                      prepareOption)
    )
}
