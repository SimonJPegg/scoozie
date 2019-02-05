package org.antipathy.scoozie.action.filesystem

import scala.xml.Elem

/**
  * Create a touch step
  * @param path the path to touch
  */
case class Touchz(path: String) extends FileSystemAction {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <touchz path={path} />
}
