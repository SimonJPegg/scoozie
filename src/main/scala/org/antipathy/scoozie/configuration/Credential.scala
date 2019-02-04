package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.XmlSerializable
import scala.xml.Elem
import scala.collection.immutable._

/**
  * Oozie credential definition
  * @param name the name of the credential
  * @param credentialsType the type of the credential
  * @param properties the credential's properties
  */
case class Credential(name: String, credentialsType: String, properties: Seq[Property]) extends XmlSerializable {

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <credential name={name} type={credentialsType}>
      {properties.map(_.toXML)}
      </credential>
}
