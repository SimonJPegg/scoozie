package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.XmlSerializable
import scala.xml.Elem

/**
  * Oozie File definition
  *
  * @param path the path to the file
  */
private[scoozie] case class File(path: String) extends XmlSerializable {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <file>{path}</file>
}
