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
  * Oozie Hive action definition
  * @param name the name of the action
  * @param scriptName the name of the hive script
  * @param scriptLocation the path to the hive script
  * @param parameters a collection of parameters to the hive script
  * @param jobXmlOption optional job.xml path
  * @param files additional files to pass to job
  * @param configuration additional config for this action
  * @param yarnConfig Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
final class HiveAction(override val name: String,
                       scriptName: String,
                       scriptLocation: String,
                       parameters: Seq[String],
                       override val jobXmlOption: Option[String],
                       files: Seq[String],
                       override val configuration: Configuration,
                       yarnConfig: YarnConfig,
                       override val prepareOption: Option[Prepare])
    extends Action
    with HasPrepare
    with HasConfig
    with HasJobXml {

  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val parametersProperties =
    buildSequenceProperties(name, "parameter", parameters)
  private val filesProperties = buildSequenceProperties(name, "file", files)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    jobXmlProperty ++
    yarnConfig.properties ++
    Map(scriptNameProperty -> scriptName, scriptLocationProperty -> scriptLocation) ++
    prepareProperties ++ parametersProperties ++ mappedProperties ++ filesProperties

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:hive-action:0.5")

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <hive xmlns={xmlns.orNull}>
        {yarnConfig.jobTrackerXML}
        {yarnConfig.nameNodeXML}
        {prepareXML}
        {jobXml}
        {configXML}
        <script>{scriptNameProperty}</script>
        {parametersProperties.keys.map(p => Param(p).toXML)}
        <file>{scriptLocationProperty}</file>
        {filesProperties.keys.map(f => File(f).toXML)}
      </hive>
}

/**
  * Companion object
  */
object HiveAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            scriptName: String,
            scriptLocation: String,
            parameters: Seq[String],
            jobXmlOption: Option[String],
            files: Seq[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new HiveAction(name,
                     scriptName,
                     scriptLocation,
                     parameters,
                     jobXmlOption,
                     files,
                     configuration,
                     yarnConfig,
                     prepareOption)
    )

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    MonadBuilder.tryOperation[Node] { () =>
      HiveAction(name = config.getString(HoconConstants.name),
                 scriptName = config.getString(HoconConstants.scriptName),
                 scriptLocation = config.getString(HoconConstants.scriptLocation),
                 parameters = Seq(config.getStringList(HoconConstants.parameters).asScala: _*),
                 jobXmlOption = ConfigurationBuilder.optionalString(config, HoconConstants.jobXml),
                 files = Seq(config.getStringList(HoconConstants.files).asScala: _*),
                 configuration = ConfigurationBuilder.buildConfiguration(config),
                 yarnConfig,
                 prepareOption = PrepareBuilder.build(config))
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${config.getString(HoconConstants.name)}", e)
    }
}
