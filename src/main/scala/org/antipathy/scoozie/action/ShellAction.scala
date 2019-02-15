/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants, MonadBuilder, PrepareBuilder}
import org.antipathy.scoozie.configuration.{Credentials, _}
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.xml.Elem

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
  * @param jobXmlOption optional job.xml path
  * @param configuration additional config for this action
  * @param yarnConfig Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
final class ShellAction(override val name: String,
                        scriptName: String,
                        scriptLocation: String,
                        commandLineArgs: Seq[String],
                        envVars: Seq[String],
                        files: Seq[String],
                        captureOutput: Boolean,
                        override val jobXmlOption: Option[String],
                        override val configuration: Configuration,
                        yarnConfig: YarnConfig,
                        override val prepareOption: Option[Prepare])
    extends Action
    with HasPrepare
    with HasConfig
    with HasJobXml {

  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val commandLineArgsProperties =
    buildSequenceProperties(name, "commandLineArgs", commandLineArgs)
  private val envVarsProperties =
    buildSequenceProperties(name, "envVars", envVars)
  private val filesProperties = buildSequenceProperties(name, "files", files)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(scriptNameProperty -> scriptName, scriptLocationProperty -> scriptLocation) ++
    prepareProperties ++
    commandLineArgsProperties ++
    envVarsProperties ++
    mappedProperties ++
    jobXmlProperty ++
    filesProperties

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:shell-action:0.2")

  /**
    * The XML for this node
    */
  override def toXML: Elem = {
    val fileVal = s"$scriptLocationProperty#$scriptNameProperty"
    <shell xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {prepareXML}
      {jobXml}
      {configXML}
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

/**
  * Companion object
  */
object ShellAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            scriptName: String,
            scriptLocation: String,
            commandLineArgs: Seq[String],
            envVars: Seq[String],
            files: Seq[String],
            captureOutput: Boolean,
            jobXmlOption: Option[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new ShellAction(name,
                      scriptName,
                      scriptLocation,
                      commandLineArgs,
                      envVars,
                      files,
                      captureOutput,
                      jobXmlOption,
                      configuration,
                      yarnConfig,
                      prepareOption)
    )

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    MonadBuilder.tryOperation[Node] { () =>
      ShellAction(name = config.getString(HoconConstants.name),
                  scriptName = config.getString(HoconConstants.scriptName),
                  scriptLocation = config.getString(HoconConstants.scriptLocation),
                  commandLineArgs = Seq(config.getStringList(HoconConstants.commandLineArguments).asScala: _*),
                  envVars = Seq(config.getStringList(HoconConstants.environmentVariables).asScala: _*),
                  files = Seq(config.getStringList(HoconConstants.files).asScala: _*),
                  captureOutput = ConfigurationBuilder.optionalBoolean(config, HoconConstants.captureOutput),
                  jobXmlOption = ConfigurationBuilder.optionalString(config, HoconConstants.jobXml),
                  configuration = ConfigurationBuilder.buildConfiguration(config),
                  yarnConfig,
                  prepareOption = PrepareBuilder.build(config))
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${config.getString(HoconConstants.name)}", e)
    }
}
