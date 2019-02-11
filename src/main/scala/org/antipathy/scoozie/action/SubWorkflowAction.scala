package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants}
import org.antipathy.scoozie.configuration.{Configuration, Credentials, YarnConfig}
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.immutable._
import scala.util._
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
    Try {
      SubWorkflowAction(name = config.getString(HoconConstants.name),
                        applicationPath = config.getString(HoconConstants.applicationPath),
                        propagateConfiguration = config.hasPath(HoconConstants.propagateConfiguration),
                        configuration = ConfigurationBuilder.buildConfiguration(config),
                        yarnConfig = yarnConfig)
    } match {
      case Success(value) => value
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString(HoconConstants.name)}")
    }
}
