package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.properties.OozieProperties
import org.antipathy.scoozie.xml.XmlSerializable
import scala.xml.Elem
import scala.collection.immutable._

/**
  * Oozie configuration definition
  *
  * @param configProperties the properties defined in this configuration
  */
private[scoozie] case class Configuration(configProperties: Seq[Property])
    extends XmlSerializable
    with OozieProperties {

  /**
    * Copy this configuration substituting the values for property names
    * @param actionName the name of the action calling this method
    * @return a copy of the configuration and its properties
    */
  private[scoozie] def withActionProperties(actionName: String): (Configuration, Map[String, String]) = {
    val mappedProps = configProperties.zipWithIndex.map {
      case (Property(name, value), index) =>
        val p = formatProperty(s"${actionName}_property$index")
        (Property(name, p), p -> value)
    }
    (this.copy(mappedProps.map(_._1)), mappedProps.map(_._2).toMap)
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem = <configuration>
    {configProperties.map(_.toXML)}
  </configuration>

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    configProperties.map {
      case Property(name, value) => name -> value
    }.toMap
}
