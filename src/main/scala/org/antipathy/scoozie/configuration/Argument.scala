package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.xml.XmlSerializable

import scala.xml.Elem

/**
  * Oozie Argument definition
  *
  * @param value the value of the Argument
  */
private[scoozie] case class Argument(value: String) extends XmlSerializable {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <argument>{value}</argument>
}
