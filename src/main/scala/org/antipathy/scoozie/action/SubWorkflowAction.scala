package org.antipathy.scoozie.action

import org.antipathy.scoozie.configuration.{Configuration, Credentials, YarnConfig}
import scala.xml.Elem
import scala.collection.immutable._
import com.typesafe.config.Config
import org.antipathy.scoozie.builder.ConfigurationBuilder
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception.ConfigurationMissingException

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
  override val xmlns: Option[String] = None

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
    try {
      SubWorkflowAction(name = config.getString("name"),
                        applicationPath = config.getString("application-path"),
                        propagateConfiguration = config.hasPath("propagate-configuration"),
                        configuration = ConfigurationBuilder.buildConfiguration(config),
                        yarnConfig = yarnConfig)
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
