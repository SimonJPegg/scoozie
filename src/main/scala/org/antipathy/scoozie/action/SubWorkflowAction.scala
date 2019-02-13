package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants, MonadBuilder}
import org.antipathy.scoozie.configuration.{Configuration, Credentials, YarnConfig}
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.immutable._
import scala.xml.Elem

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
                              override val configuration: Configuration,
                              yarnConfig: YarnConfig)
    extends Action
    with HasConfig {

  private val applicationPathProperty = formatProperty(s"${name}_applicationPath")

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = None

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map(applicationPathProperty -> applicationPath) ++ mappedProperties

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
              configXML
            }
        }
      </sub-workflow>
}

/**
  * Companion object
  */
object SubWorkflowAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            applicationPath: String,
            propagateConfiguration: Boolean,
            configuration: Configuration,
            yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]): Node =
    Node(new SubWorkflowAction(name, applicationPath, propagateConfiguration, configuration, yarnConfig))

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    MonadBuilder.tryOperation[Node] { () =>
      SubWorkflowAction(name = config.getString(HoconConstants.name),
                        applicationPath = config.getString(HoconConstants.applicationPath),
                        propagateConfiguration = config.hasPath(HoconConstants.propagateConfiguration),
                        configuration = ConfigurationBuilder.buildConfiguration(config),
                        yarnConfig = yarnConfig)
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${config.getString(HoconConstants.name)}", e)
    }
}
