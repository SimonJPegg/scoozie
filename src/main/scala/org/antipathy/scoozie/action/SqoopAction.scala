package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.{Configuration, YarnConfig}
import scala.collection.immutable._
import scala.xml.Elem
import org.antipathy.scoozie.configuration.Arg
import org.antipathy.scoozie.configuration.File
import org.antipathy.scoozie.configuration.Credentials
import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{ConfigurationBuilder, PrepareBuilder}
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception.ConfigurationMissingException

/**
  * Oozie Sqoop action definition
  * @param name the name of the action
  * @param command an optional sqoop command (default)
  * @param args arguments to specify to sqoop (ignored if command is specified)
  * @param files files to include with the action
  * @param configuration configuration to provide to the action
  * @param yarnConfig the yarn configuration
  * @param prepareOption an optional prepare step
  */
class SqoopAction(override val name: String,
                  command: Option[String],
                  args: Seq[String],
                  files: Seq[String],
                  configuration: Configuration,
                  yarnConfig: YarnConfig,
                  prepareOption: Option[Prepare])
    extends Action {

  private val argsProperties = buildSequenceProperties(name, "arguments", args)
  private val filesProperties = buildSequenceProperties(name, "files", files)
  private val commandProperty = formatProperty(s"${name}_command")

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
    command.map(prop => Map(commandProperty -> prop)).getOrElse(Map())

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
      {if (prepareOptionMapped.isDefined) {
        prepareOptionMapped.get.toXML
      }
      }
      {if (configurationProperties._1.configProperties.nonEmpty) {
          configurationProperties._1.toXML
        }
      }
      { if (command.isDefined) {
          <command>{commandProperty}</command>
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
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(new SqoopAction(name, command, args, files, configuration, yarnConfig, prepareOption))

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    try {
      SqoopAction(name = config.getString("name"),
                  command = if (config.hasPath("command")) Some(config.getString("command")) else None,
                  args =
                    if (config.hasPath("command")) Seq(config.getStringList("command-line-arguments").asScala: _*)
                    else Seq(),
                  files = Seq(config.getStringList("files").asScala: _*),
                  configuration = ConfigurationBuilder.buildConfiguration(config),
                  yarnConfig = yarnConfig,
                  prepareOption = PrepareBuilder.build(config))
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
