package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.{Configuration, YarnConfig}
import scala.collection.immutable._
import scala.xml.Elem
import org.antipathy.scoozie.configuration.Arg
import org.antipathy.scoozie.configuration.File
import org.antipathy.scoozie.Node
import org.antipathy.scoozie.configuration.Credentials

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

object SqoopAction {

  def apply(name: String,
            command: Option[String],
            args: Seq[String],
            files: Seq[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(new SqoopAction(name, command, args, files, configuration, yarnConfig, prepareOption))
}
