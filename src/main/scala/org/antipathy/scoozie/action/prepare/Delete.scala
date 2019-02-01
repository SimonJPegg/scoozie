package org.antipathy.scoozie.action.prepare

import scala.xml.Elem

/**
  * Oozie prepare delete definition
  *
  * @param path the path to delete
  */
case class Delete(override val path: String) extends PrepareFSAction {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <delete path={path} />
}
