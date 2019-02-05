package org.antipathy.scoozie.action

import org.antipathy.scoozie.configuration.{Configuration, Credentials, YarnConfig}
import org.antipathy.scoozie.Node

import scala.xml.Elem
import scala.collection.immutable._

/**
  * Oozie sub-workflow action definition
  * @param name the name of the action
  * @param applicationPath The path to the workflow
  * @param propagateConfiguration should the parent workflow properties be used
  * @param configuration configuration to provide to the action
  * @param yarnConfig the yarn configuration
  */
final class SubWorkflowAction(override val name: String,
                              applicationPath: String,
                              propagateConfiguration: Boolean,
                              configuration: Configuration,
                              yarnConfig: YarnConfig)
    extends Action {

  private val applicationPathProperty = formatProperty(s"${name}_applicationPath")
  private val mappedConfigAndProperties = configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties._1

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:ssh-action:0.2")

  /**
    * Get the Oozie properties for this object
    */
  override val properties
    : Map[String, String] = Map(applicationPathProperty -> applicationPath) ++ mappedConfigAndProperties._2

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <sub-workflow>
        <app-path>{applicationPathProperty}</app-path>
        { if(propagateConfiguration) {
            <propagate-configuration/>
          }
        }
        {if (mappedConfig.configProperties.nonEmpty) {
              mappedConfig.toXML
            }
        }
      </sub-workflow>
}

object SubWorkflowAction {

  def apply(name: String,
            applicationPath: String,
            propagateConfiguration: Boolean,
            configuration: Configuration,
            yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]): Node =
    Node(new SubWorkflowAction(name, applicationPath, propagateConfiguration, configuration, yarnConfig))
}
