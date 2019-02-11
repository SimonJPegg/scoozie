package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.builder.{ConfigurationBuilder, PrepareBuilder}
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.util._
import scala.xml.Elem

/**
  * Oozie Sqoop action definition
  * @param name the name of the action
  * @param command an optional sqoop command (default)
  * @param args arguments to specify to sqoop (ignored if command is specified)
  * @param files files to include with the action
  * @param jobXmlOption optional job.xml path
  * @param configuration configuration to provide to the action
  * @param yarnConfig the yarn configuration
  * @param prepareOption an optional prepare step
  */
class SqoopAction(override val name: String,
                  command: Option[String],
                  args: Seq[String],
                  files: Seq[String],
                  jobXmlOption: Option[String],
                  configuration: Configuration,
                  yarnConfig: YarnConfig,
                  prepareOption: Option[Prepare])
    extends Action {

  <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>

  private val argsProperties = buildSequenceProperties(name, "arguments", args)
  private val filesProperties = buildSequenceProperties(name, "files", files)
  private val commandProperty = buildStringOptionProperty(name, "command", command)
  private val jobXmlProperty =
    buildStringOptionProperty(name, "jobXml", jobXmlOption)
  private val configurationProperties = configuration.withActionProperties(name)
  private val prepareOptionAndProps = prepareOption.map(_.withActionProperties(name))
  private val prepareProperties = prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    argsProperties ++
    filesProperties ++
    configurationProperties._2 ++
    prepareProperties ++
    jobXmlProperty ++
    commandProperty

  /**
    * The XML namespace for an action element
    */
  override def xmlns: Option[String] = Some("uri:oozie:sqoop-action:0.3")

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <sqoop xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {prepareOptionMapped.map(_.toXML).orNull}
      {if (jobXmlOption.isDefined) {
          <job-xml>{jobXmlProperty.keys}</job-xml>
        }
      }
      {if (configurationProperties._1.configProperties.nonEmpty) {
          configurationProperties._1.toXML
        }
      }
      { if (command.isDefined) {
          <command>{commandProperty.keys}</command>
        } else {
          argsProperties.keys.map(Arg(_).toXML)
        }
      }
      {filesProperties.keys.map(File(_).toXML)}
    </sqoop>
}

/**
  * Companion object
  */
object SqoopAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            command: Option[String],
            args: Seq[String],
            files: Seq[String],
            jobXmlOption: Option[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(new SqoopAction(name, command, args, files, jobXmlOption, configuration, yarnConfig, prepareOption))

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    Try {
      SqoopAction(name = config.getString("name"),
                  command = if (config.hasPath("command")) Some(config.getString("command")) else None,
                  args =
                    if (config.hasPath("command")) Seq(config.getStringList("command-line-arguments").asScala: _*)
                    else Seq(),
                  files = Seq(config.getStringList("files").asScala: _*),
                  jobXmlOption = if (config.hasPath("job-xml")) {
                    Some(config.getString("job-xml"))
                  } else None,
                  configuration = ConfigurationBuilder.buildConfiguration(config),
                  yarnConfig = yarnConfig,
                  prepareOption = PrepareBuilder.build(config))
    } match {
      case Success(value) => value
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString("name")}")
    }
}
