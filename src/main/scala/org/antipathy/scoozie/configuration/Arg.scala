package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.xml.XmlSerializable

import scala.xml.Elem

/**
  * Oozie Arg definition
  *
  * @param value the value of the arg
  */
private[scoozie] case class Arg(value: String) extends XmlSerializable {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <arg>{value}</arg>
}
