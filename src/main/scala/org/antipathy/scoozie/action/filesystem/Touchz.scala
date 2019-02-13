package org.antipathy.scoozie.action.filesystem

import org.antipathy.scoozie.properties.PropertyFormatter

import scala.xml.Elem

/**
  * Create a touch step
  * @param path the path to touch
  */
case class Touchz(path: String) extends FileSystemAction with PropertyFormatter {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <touchz path={formatProperty(path)} />
}
