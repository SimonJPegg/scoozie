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
import org.antipathy.scoozie.configuration.File

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
                       jobXmlOption: Option[String],
                       files: Seq[String],
                       configuration: Configuration,
                       yarnConfig: YarnConfig,
                       prepareOption: Option[Prepare])
    extends Action {

  private val jobXMLProperty = formatProperty(s"${name}_jobXML")
  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val parametersProperties =
    buildSequenceProperties(name, "parameter", parameters)
  private val filesProperties = buildSequenceProperties(name, "file", files)
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
  override def properties: Map[String, String] = {

    val jobXML = if (jobXmlOption.isDefined) {
      Map(jobXMLProperty -> jobXmlOption.get)
    } else { Map() }
    yarnConfig.properties ++ Map(scriptNameProperty -> scriptName, scriptLocationProperty -> scriptLocation) ++ prepareProperties ++ parametersProperties ++ mappedConfigAndProperties._2 ++ filesProperties ++ jobXML
  }

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
        {if (prepareOptionMapped.isDefined) {
            prepareOptionMapped.get.toXML
          }
        }
        {if (jobXmlOption.isDefined) {
              <job-xml>{jobXMLProperty}</job-xml>
            }
        }
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
        {filesProperties.map(f => File(f._1).toXML)}
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
    try {
      HiveAction(name = config.getString("name"),
                 scriptName = config.getString("script-name"),
                 scriptLocation = config.getString("script-location"),
                 parameters = Seq(config.getStringList("parameters").asScala: _*),
                 jobXmlOption = if (config.hasPath("job-xml")) {
                   Some(config.getString("job-xml"))
                 } else None,
                 files = Seq(config.getStringList("files").asScala: _*),
                 configuration = ConfigurationBuilder.buildConfiguration(config),
                 yarnConfig,
                 prepareOption = PrepareBuilder.build(config))
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
