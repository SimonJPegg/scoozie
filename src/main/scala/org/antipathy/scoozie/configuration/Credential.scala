package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.xml.XmlSerializable

import scala.collection.immutable._
import scala.xml.Elem

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
