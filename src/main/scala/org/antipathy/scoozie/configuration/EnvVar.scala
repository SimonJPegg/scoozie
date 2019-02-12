package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.xml.XmlSerializable

import scala.xml.Elem

/**
  * Oozie EnvVar definition
  *
  * @param value the value of the EnvVar
  */
private[scoozie] case class EnvVar(value: String) extends XmlSerializable {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <env-var>{value}</env-var>
}
