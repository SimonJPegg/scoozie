package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration._
import scala.xml.Elem
import scala.collection.immutable._
import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{ConfigurationBuilder, PrepareBuilder}
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception.ConfigurationMissingException

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
  private val mappedConfig = mappedConfigAndProperties._1
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
    mappedConfigAndProperties._2 ++ argumentsProperties ++ filesProperties

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = None

  <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
      <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <pig>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {if (prepareOptionMapped.isDefined) {
          prepareOptionMapped.get.toXML
        }
      }
      {if (jobXmlOption.isDefined) {
          <job-xml>{jobXmlProperty}</job-xml>
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
    try {
      PigAction(name = config.getString("name"),
                script = config.getString("script"),
                params = Seq(config.getStringList("params").asScala: _*),
                arguments = Seq(config.getStringList("arguments").asScala: _*),
                files = Seq(config.getStringList("files").asScala: _*),
                jobXmlOption = if (config.hasPath("job-xml")) Some(config.getString("job-xml")) else None,
                configuration = ConfigurationBuilder.buildConfiguration(config),
                yarnConfig,
                prepareOption = PrepareBuilder.build(config))
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
