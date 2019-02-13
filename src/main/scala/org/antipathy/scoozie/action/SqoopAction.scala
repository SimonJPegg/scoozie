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
                  override val jobXmlOption: Option[String],
                  override val configuration: Configuration,
                  yarnConfig: YarnConfig,
                  override val prepareOption: Option[Prepare])
    extends Action
    with HasPrepare
    with HasConfig
    with HasJobXml {

  private val argsProperties = buildSequenceProperties(name, "arguments", args)
  private val filesProperties = buildSequenceProperties(name, "files", files)
  private val commandProperty = buildStringOptionProperty(name, "command", command)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    argsProperties ++
    filesProperties ++
    configurationProperties.properties ++
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
      {prepareXML}
      {jobXml}
      {configXML}
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
    MonadBuilder.tryOperation[Node] { () =>
      SqoopAction(name = config.getString(HoconConstants.name),
                  command = ConfigurationBuilder.optionalString(config, HoconConstants.command),
                  args =
                    if (!config.hasPath(HoconConstants.command))
                      Seq(config.getStringList(HoconConstants.commandLineArguments).asScala: _*)
                    else Seq(),
                  files = Seq(config.getStringList(HoconConstants.files).asScala: _*),
                  jobXmlOption = ConfigurationBuilder.optionalString(config, HoconConstants.jobXml),
                  configuration = ConfigurationBuilder.buildConfiguration(config),
                  yarnConfig = yarnConfig,
                  prepareOption = PrepareBuilder.build(config))
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${config.getString(HoconConstants.name)}", e)
    }
}
