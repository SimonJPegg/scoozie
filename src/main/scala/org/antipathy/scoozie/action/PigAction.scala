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
                jobXmlOption: Option[String],
                configuration: Configuration,
                yarnConfig: YarnConfig,
                prepareOption: Option[Prepare])
    extends Action {

  private val scriptProperty = formatProperty(s"${name}_script")
  private val paramsProperties = buildSequenceProperties(name, "param", params)
  private val argumentsProperties = buildSequenceProperties(name, "arg", arguments)
  private val filesProperties = buildSequenceProperties(name, "file", files)
  private val jobXmlProperty =
    buildStringOptionProperty(name, "jobXml", jobXmlOption)
  private val mappedConfigAndProperties =
    configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties.config
  private val prepareOptionAndProps =
    prepareOption.map(_.withActionProperties(name))
  private val prepareProperties =
    prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(scriptProperty -> script) ++
    paramsProperties ++
    jobXmlProperty ++
    prepareProperties ++
    mappedConfigAndProperties.properties ++ argumentsProperties ++ filesProperties

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
      {prepareOptionMapped.map(_.toXML).orNull}
      {if (jobXmlOption.isDefined) {
          <job-xml>{jobXmlProperty.keys}</job-xml>
        }
      }

      {if (mappedConfig.configProperties.nonEmpty) {
          mappedConfig.toXML
        }
      }
      <script>{scriptProperty}</script>
      {paramsProperties.map(p => Param(p._1).toXML)}
      {argumentsProperties.map(p => Argument(p._1).toXML)}
      {filesProperties.map(f => File(f._1).toXML)}
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
    Try {
      PigAction(name = config.getString(HoconConstants.name),
                script = config.getString(HoconConstants.script),
                params = Seq(config.getStringList(HoconConstants.params).asScala: _*),
                arguments = Seq(config.getStringList(HoconConstants.arguments).asScala: _*),
                files = Seq(config.getStringList(HoconConstants.files).asScala: _*),
                jobXmlOption =
                  if (config.hasPath(HoconConstants.jobXml)) Some(config.getString(HoconConstants.jobXml)) else None,
                configuration = ConfigurationBuilder.buildConfiguration(config),
                yarnConfig,
                prepareOption = PrepareBuilder.build(config))
    } match {
      case Success(value) => value
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString(HoconConstants.name)}")
    }
}
