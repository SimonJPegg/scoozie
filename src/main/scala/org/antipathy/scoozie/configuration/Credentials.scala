package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.properties.OozieProperties
import org.antipathy.scoozie.xml.XmlSerializable
import scala.xml.Elem
import scala.collection.immutable.Map

/**
  * Oozie credentials definition
  * @param credential the credential definitions
  */
case class Credentials(credential: Credential) extends XmlSerializable with OozieProperties {

  /**
    * Copy this configuration substituting the values for property names
    * @param actionName the name of the action calling this method
    * @return a copy of the configuration and its properties
    */
  private[scoozie] def withActionProperties(actionName: String): (Credentials, Map[String, String]) = {
    val mappedProps = credential.properties.sortBy(_.name).zipWithIndex.map {
      case (Property(name, value), index) =>
        val p = formatProperty(s"${actionName}_credentialProperty$index")
        (Property(name, p), p -> value)
    }
    (this.copy(credential.copy(properties = mappedProps.map(_._1))), mappedProps.map(_._2).toMap)
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <credentials>
      {credential.toXML}
    </credentials>

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    credential.properties.map {
      case Property(name, value) => name -> value
    }.toMap

}
