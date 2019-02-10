package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.{Configuration, Credentials, YarnConfig}
import scala.xml.Elem
import scala.collection.immutable._
import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{ConfigurationBuilder, PrepareBuilder}
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception.ConfigurationMissingException

/**
  * Oozie Hive action definition
  * @param name the name of the action
  * @param hiveSettingsXML the path to the hive settings XML
  * @param scriptName the name of the hive script
  * @param scriptLocation the path to the hive script
  * @param parameters a collection of parameters to the hive script
  * @param configuration additional config for this action
  * @param yarnConfig Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
final class HiveAction(override val name: String,
                       hiveSettingsXML: String,
                       scriptName: String,
                       scriptLocation: String,
                       parameters: Seq[String],
                       configuration: Configuration,
                       yarnConfig: YarnConfig,
                       prepareOption: Option[Prepare] = None)
    extends Action {

  private val hiveSettingsXMLProperty = formatProperty(s"${name}_hiveSettingsXML")
  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val parametersProperties =
    buildSequenceProperties(name, "parameter", parameters)
  private val prepareOptionAndProps =
    prepareOption.map(_.withActionProperties(name))
  private val prepareProperties =
    prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)
  private val mappedConfigAndProperties = configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties._1

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    yarnConfig.properties ++ Map(
      hiveSettingsXMLProperty -> hiveSettingsXML,
      scriptNameProperty -> scriptName,
      scriptLocationProperty -> scriptLocation
    ) ++ prepareProperties ++ parametersProperties ++ mappedConfigAndProperties._2

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:hive-action:0.2")

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <hive xmlns={xmlns.orNull}>
        {yarnConfig.jobTrackerXML}
        {yarnConfig.nameNodeXML}
        {if (prepareOptionMapped.isDefined) {
            prepareOptionMapped.get.toXML
          }
        }
        <job-xml>{hiveSettingsXMLProperty}</job-xml>
        {if (mappedConfig.configProperties.nonEmpty) {
            mappedConfig.toXML
          }
        }
        <script>{scriptNameProperty}</script>
        {parametersProperties.map{
            case (key,_) => <param>{key}</param>
          }
        }
        <file>{scriptLocationProperty}</file>
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
            hiveSettingsXML: String,
            scriptName: String,
            scriptLocation: String,
            parameters: Seq[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new HiveAction(name,
                     hiveSettingsXML,
                     scriptName,
                     scriptLocation,
                     parameters,
                     configuration,
                     yarnConfig,
                     prepareOption)
    )

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    try {
      HiveAction(name = config.getString("name"),
                 hiveSettingsXML = config.getString("hive-settings-xml"),
                 scriptName = config.getString("script-name"),
                 scriptLocation = config.getString("script-location"),
                 parameters = Seq(config.getStringList("parameters").asScala: _*),
                 configuration = ConfigurationBuilder.buildConfiguration(config),
                 yarnConfig,
                 prepareOption = PrepareBuilder.build(config))
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
