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
  * Oozie Java action definition
  * @param name the name of the action
  * @param script the location of the pig script
  * @param params parameters to the script
  * @param arguments arguments to the script
  * @param files additional files to bundle with the job
  * @param jobXmlOption optional job.xml for the script
  * @param configuration additional config for this action
  * @param yarnConfig Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
class PigAction(override val name: String,
                script: String,
                params: Seq[String],
                arguments: Seq[String],
                files: Seq[String],
                override val jobXmlOption: Option[String],
                override val configuration: Configuration,
                yarnConfig: YarnConfig,
                override val prepareOption: Option[Prepare])
    extends Action
    with HasPrepare
    with HasConfig
    with HasJobXml {

  private val scriptProperty = formatProperty(s"${name}_script")
  private val paramsProperties = buildSequenceProperties(name, "param", params)
  private val argumentsProperties = buildSequenceProperties(name, "arg", arguments)
  private val filesProperties = buildSequenceProperties(name, "file", files)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(scriptProperty -> script) ++
    paramsProperties ++
    jobXmlProperty ++
    prepareProperties ++
    mappedProperties ++
    argumentsProperties ++
    filesProperties

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <pig>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {prepareXML}
      {jobXml}
      {configXML}
      <script>{scriptProperty}</script>
      {paramsProperties.keys.map(p => Param(p).toXML)}
      {argumentsProperties.keys.map(p => Argument(p).toXML)}
      {filesProperties.keys.map(f => File(f).toXML)}
    </pig>
}

/**
  * Companion object
  */
object PigAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            script: String,
            params: Seq[String],
            arguments: Seq[String],
            files: Seq[String],
            jobXmlOption: Option[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(new PigAction(name, script, params, arguments, files, jobXmlOption, configuration, yarnConfig, prepareOption))

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    MonadBuilder.tryOperation[Node] { () =>
      PigAction(name = config.getString(HoconConstants.name),
                script = config.getString(HoconConstants.script),
                params = Seq(config.getStringList(HoconConstants.params).asScala: _*),
                arguments = Seq(config.getStringList(HoconConstants.arguments).asScala: _*),
                files = Seq(config.getStringList(HoconConstants.files).asScala: _*),
                jobXmlOption = ConfigurationBuilder.optionalString(config, HoconConstants.jobXml),
                configuration = ConfigurationBuilder.buildConfiguration(config),
                yarnConfig,
                prepareOption = PrepareBuilder.build(config))
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${config.getString(HoconConstants.name)}", e)
    }
}
