package org.antipathy.scoozie.action

import scala.xml.Elem
import org.antipathy.scoozie.configuration.Args
import scala.collection.immutable._
import org.antipathy.scoozie.Node
import org.antipathy.scoozie.configuration.Credentials

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
  override val properties: Map[String, String] = Map(
    hostProperty -> host,
    commandProperty -> command
  ) ++ argsProperty

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

object SshAction {

  def apply(
      name: String,
      host: String,
      command: String,
      args: Seq[String],
      captureOutput: Boolean
  )(implicit credentialsOption: Option[Credentials]): Node =
    Node(new SshAction(name, host, command, args, captureOutput))
}
