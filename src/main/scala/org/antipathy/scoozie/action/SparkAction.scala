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
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.xml.Elem

/**
  * Oozie Spark action definition
  * @param name the name of the action
  * @param sparkMasterURL the url of the spark master
  * @param sparkMode the mode the spark job should run in
  * @param sparkJobName the name of the spark job
  * @param mainClass the main class of the spark job
  * @param sparkJar the location of the spark jar
  * @param sparkOptions options for the spark job
  * @param commandLineArgs command line arguments for the spark job
  * @param jobXmlOption optional job.xml path
  * @param prepareOption an optional prepare phase for the action
  * @param configuration additional config for this action
  * @param yarnConfig Yarn configuration for this action
  */
final class SparkAction(override val name: String,
                        sparkMasterURL: String,
                        sparkMode: String,
                        sparkJobName: String,
                        mainClass: String,
                        sparkJar: String,
                        sparkOptions: String,
                        commandLineArgs: Seq[String],
                        override val jobXmlOption: Option[String],
                        override val prepareOption: Option[Prepare],
                        override val configuration: Configuration,
                        yarnConfig: YarnConfig)
    extends Action
    with HasPrepare
    with HasConfig
    with HasJobXml {

  private val sparkMasterURLProperty = formatProperty(s"${name}_sparkMasterURL")
  private val sparkModeProperty = formatProperty(s"${name}_sparkMode")
  private val sparkJobNameProperty = formatProperty(s"${name}_sparkJobName")
  private val mainClassProperty = formatProperty(s"${name}_mainClass")
  private val sparkJarProperty = formatProperty(s"${name}_sparkJar")
  private val sparkOptionsProperty = formatProperty(s"${name}_sparkOptions")
  private val commandLineArgsProperties =
    buildSequenceProperties(name, "commandLineArg", commandLineArgs)

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:spark-action:0.1")

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(sparkMasterURLProperty -> sparkMasterURL,
        sparkModeProperty -> sparkMode,
        sparkJobNameProperty -> sparkJobName,
        mainClassProperty -> mainClass,
        sparkJarProperty -> sparkJar,
        sparkOptionsProperty -> sparkOptions) ++
    commandLineArgsProperties ++
    prepareProperties ++
    mappedProperties ++
    jobXmlProperty

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <spark xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {prepareXML}
      {jobXml}
      {configXML}
      <master>{sparkMasterURLProperty}</master>
      <mode>{sparkModeProperty}</mode>
      <name>{sparkJobNameProperty}</name>
      <class>{mainClassProperty}</class>
      <jar>{sparkJarProperty}</jar>
      <spark-opts>{sparkOptionsProperty}</spark-opts>
      {commandLineArgsProperties.keys.map(Arg(_).toXML)}
    </spark>
}

/**
  * Companion object
  */
object SparkAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            sparkMasterURL: String,
            sparkMode: String,
            sparkJobName: String,
            mainClass: String,
            sparkJar: String,
            sparkOptions: String,
            commandLineArgs: Seq[String],
            jobXmlOption: Option[String],
            prepareOption: Option[Prepare],
            configuration: Configuration,
            yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new SparkAction(name,
                      sparkMasterURL,
                      sparkMode,
                      sparkJobName,
                      mainClass,
                      sparkJar,
                      sparkOptions,
                      commandLineArgs,
                      jobXmlOption,
                      prepareOption,
                      configuration,
                      yarnConfig)
    )

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    MonadBuilder.tryOperation[Node] { () =>
      SparkAction(name = config.getString(HoconConstants.name),
                  sparkMasterURL = config.getString(HoconConstants.sparkMasterUrl),
                  sparkMode = config.getString(HoconConstants.sparkMode),
                  sparkJobName = config.getString(HoconConstants.sparkJobName),
                  mainClass = config.getString(HoconConstants.mainClass),
                  sparkJar = config.getString(HoconConstants.sparkJar),
                  sparkOptions = config.getString(HoconConstants.sparkOptions),
                  commandLineArgs = Seq(config.getStringList(HoconConstants.commandLineArguments).asScala: _*),
                  jobXmlOption = ConfigurationBuilder.optionalString(config, HoconConstants.jobXml),
                  configuration = ConfigurationBuilder.buildConfiguration(config),
                  yarnConfig = yarnConfig,
                  prepareOption = PrepareBuilder.build(config))
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${config.getString(HoconConstants.name)}", e)
    }
}
