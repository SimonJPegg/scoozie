package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.{Argument, Configuration, Credentials, YarnConfig}
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
  * @param params arguments to the script
  * @param jobXml optional job.xml for the script
  * @param configuration additional config for this action
  * @param yarnConfig Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
class PigAction(override val name: String,
                script: String,
                params: Seq[String],
                jobXml: Option[String] = None,
                configuration: Configuration,
                yarnConfig: YarnConfig,
                prepareOption: Option[Prepare] = None)
    extends Action {

  private val scriptProperty = formatProperty(s"${name}_script")
  private val paramsProperties = buildSequenceProperties(name, "param", params)
  private val jobXmlProperty =
    buildStringOptionProperty(name, "jobXml", jobXml)
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
    mappedConfigAndProperties._2

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:workflow:0.2")

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
      {if (jobXml.isDefined) {
          <job-xml>{jobXmlProperty}</job-xml>
        }
      }

      {if (mappedConfig.configProperties.nonEmpty) {
          mappedConfig.toXML
        }
      }
      <script>{scriptProperty}</script>
      {
        val paramSeq = Seq.fill(params.length)("-param")
        paramSeq.zipAll(paramsProperties.keys,"","").flatMap{
            case (a,b) => Seq(a,b) 
        }.map(Argument(_).toXML)
      }
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
            jobXml: Option[String] = None,
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
    Node(new PigAction(name, script, params, jobXml, configuration, yarnConfig, prepareOption))

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    try {
      PigAction(name = config.getString("name"),
                script = config.getString("script"),
                params = Seq(config.getStringList("params").asScala: _*),
                jobXml = if (config.hasPath("job-xml")) Some(config.getString("job-xml")) else None,
                configuration = ConfigurationBuilder.buildConfiguration(config),
                yarnConfig,
                prepareOption = PrepareBuilder.build(config))
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
