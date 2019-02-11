package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.builder.HoconConstants
import org.antipathy.scoozie.configuration.Args
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.util._
import scala.xml.Elem

/**
  * Oozie SSH action
  *
  * @param name The name of the action
  * @param host The hos to connect to (user@host)
  * @param command The shell command to execute
  * @param args Parameters to be passed to the shell command
  * @param captureOutput Capture output of the STDOUT of the ssh command execution
  */
final class SshAction(override val name: String,
                      host: String,
                      command: String,
                      args: Seq[String],
                      captureOutput: Boolean)
    extends Action {

  private val hostProperty = formatProperty(s"${name}_host")
  private val commandProperty = formatProperty(s"${name}_command")
  private val argsProperty = buildSequenceProperties(name, "arg", args)

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:ssh-action:0.2")

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map(hostProperty -> host, commandProperty -> command) ++ argsProperty

  /**
    * Does this action require yarn credentials in Kerberos environments
    */
  override def requiresCredentials: Boolean = false

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <ssh xmlns={xmlns.orNull}>
        <host>{hostProperty}</host>
        <command>{commandProperty}</command>
        {argsProperty.keys.map(Args(_).toXML)}
        { if (captureOutput) {
            <capture-output/>
          }
        }
      </ssh>
}

/**
  * Companion object
  */
object SshAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String, host: String, command: String, args: Seq[String], captureOutput: Boolean): Node =
    Node(new SshAction(name, host, command, args, captureOutput))(None)

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config): Node =
    Try {
      SshAction(name = config.getString(HoconConstants.name),
                host = config.getString(HoconConstants.host),
                command = config.getString(HoconConstants.command),
                captureOutput = if (config.hasPath(HoconConstants.captureOutput)) {
                  config.getBoolean(HoconConstants.captureOutput)
                } else false,
                args = Seq(config.getStringList(HoconConstants.commandLineArguments).asScala: _*))
    } match {
      case Success(value) => value
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString(HoconConstants.name)}")
    }
}
