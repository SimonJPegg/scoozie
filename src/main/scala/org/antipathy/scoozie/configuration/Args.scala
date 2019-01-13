package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.XmlSerializable
import scala.xml.Elem

/**
  * Oozie Arg definition
  *
  * @param value the value of the arg
  */
private[scoozie] case class Args(value: String) extends XmlSerializable {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <args>{value}</args>
}
