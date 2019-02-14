// $COVERAGE-OFF$
package org.antipathy.scoozie.action

import org.antipathy.scoozie.configuration.{ActionProperties, Configuration}

import scala.collection.immutable.Map
import scala.xml.Elem
import org.antipathy.scoozie.configuration.Property
import org.antipathy.scoozie.properties.PropertyFormatter

/**
  * Trait for building config properties
  */
trait HasConfig extends PropertyFormatter {
  this: Nameable =>
  import scala.collection.immutable

  def configuration: Configuration

  protected val actionConfig: ActionProperties[Configuration] =
    Configuration(configuration.configProperties.filter { p =>
      !p.value.contains("coord:") &&
      !p.value.contains("wf:")
    }).withActionProperties(name)
  protected val asIsConfig: immutable.Seq[Property] =
    configuration.configProperties.filter { p =>
      p.value.contains("coord:") ||
      p.value.contains("wf:")
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
