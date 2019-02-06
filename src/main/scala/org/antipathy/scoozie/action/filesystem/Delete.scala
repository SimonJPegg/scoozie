package org.antipathy.scoozie.action.filesystem

import org.antipathy.scoozie.action.prepare.PrepareFSAction
import scala.xml.Elem

/**
  * Oozie delete definition
  *
  * @param path the path to delete
  */
case class Delete(override val path: String) extends PrepareFSAction with FileSystemAction {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <delete path={path} />
}
