package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants, PrepareBuilder}
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.util._
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
                       jobXmlOption: Option[String],
                       files: Seq[String],
                       configuration: Configuration,
                       yarnConfig: YarnConfig,
                       prepareOption: Option[Prepare])
    extends Action {

  private val jobXmlProperty = buildStringOptionProperty(name, "jobXml", jobXmlOption)
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
  override def properties: Map[String, String] =
    jobXmlProperty ++
    yarnConfig.properties ++
    Map(scriptNameProperty -> scriptName, scriptLocationProperty -> scriptLocation) ++
    prepareProperties ++ parametersProperties ++ mappedConfigAndProperties._2 ++ filesProperties

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
        {prepareOptionMapped.map(_.toXML).orNull}
        {if (jobXmlOption.isDefined) {
              <job-xml>{jobXmlProperty.keys}</job-xml>
            }
        }
        {if (mappedConfig.configProperties.nonEmpty) {
            mappedConfig.toXML
          }
        }
        <script>{scriptNameProperty}</script>
        {parametersProperties.map(p => Param(p._1).toXML)}
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
    Try {
      HiveAction(name = config.getString(HoconConstants.name),
                 scriptName = config.getString(HoconConstants.scriptName),
                 scriptLocation = config.getString(HoconConstants.scriptLocation),
                 parameters = Seq(config.getStringList(HoconConstants.parameters).asScala: _*),
                 jobXmlOption = if (config.hasPath(HoconConstants.jobXml)) {
                   Some(config.getString(HoconConstants.jobXml))
                 } else None,
                 files = Seq(config.getStringList(HoconConstants.files).asScala: _*),
                 configuration = ConfigurationBuilder.buildConfiguration(config),
                 yarnConfig,
                 prepareOption = PrepareBuilder.build(config))
    } match {
      case Success(node) => node
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString(HoconConstants.name)}")
    }
}
