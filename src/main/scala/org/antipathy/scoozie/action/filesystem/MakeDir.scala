package org.antipathy.scoozie.action.filesystem
import org.antipathy.scoozie.action.prepare.PrepareFSAction

import scala.xml.Elem

/**
  * Oozie make directory definition
  *
  * @param path the path to create
  */
case class MakeDir(override val path: String) extends PrepareFSAction with FileSystemAction {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <mkdir path={path} />
}
