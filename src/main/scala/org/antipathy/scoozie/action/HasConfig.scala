// $COVERAGE-OFF$
package org.antipathy.scoozie.action

import org.antipathy.scoozie.configuration.{ActionProperties, Configuration}

import scala.xml.Elem

/**
  * Trait for building config properties
  */
trait HasConfig {
  this: Nameable =>

  def configuration: Configuration

  //map configuration to action name
  protected val configurationProperties: ActionProperties[Configuration] = configuration.withActionProperties(name)
  protected val mappedConfig: Configuration = configurationProperties.mappedType
  protected val mappedProperties: Map[String, String] = configurationProperties.properties

  /**
    * Render the XML for this config
    */
  protected def configXML: Elem =
    (if (mappedConfig.configProperties.nonEmpty) {
       Some(mappedConfig.toXML)
     } else None).orNull

}
// $COVERAGE-ON$
