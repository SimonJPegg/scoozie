// $COVERAGE-OFF$
package org.antipathy.scoozie.action

import org.antipathy.scoozie.configuration.{ActionProperties, Configuration, Property}
import org.antipathy.scoozie.properties.PropertyFormatter

import scala.collection.immutable._
import scala.xml.Elem

/**
  * Trait for building config properties
  */
trait HasConfig extends PropertyFormatter {
  this: Nameable =>

  private val coordFunctionPrefix: String = "coord:"
  private val wfFunctionPrefix: String = "wf:"

  def configuration: Configuration

  protected val actionConfig: ActionProperties[Configuration] =
    Configuration(configuration.configProperties.filter { p =>
      !p.value.contains(coordFunctionPrefix) &&
      !p.value.contains(wfFunctionPrefix)
    }).withActionProperties(name)
  protected val asIsConfig: Seq[Property] =
    configuration.configProperties.filter { p =>
      p.value.contains(coordFunctionPrefix) ||
      p.value.contains(wfFunctionPrefix)
    }.map(p => Property(p.name, formatProperty(p.value.replace("\"", ""))))

  //map configuration to action name
  protected val mappedConfig: Configuration = actionConfig.mappedType
  protected val mappedProperties: Map[String, String] = actionConfig.properties

  /**
    * Render the XML for this config
    */
  protected def configXML: Elem = {
    if (configuration.configProperties.nonEmpty)
      Some {
        <configuration>
          {(actionConfig.mappedType.configProperties ++ asIsConfig).map(_.toXML)}
        </configuration>
      } else None
  }.orNull

}
// $COVERAGE-ON$
