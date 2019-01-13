package org.antipathy.scoozie.action.prepare

import scala.xml.Elem

/**
  * Oozie prepare make directory definition
  *
  * @param path the path to create
  */
case class MakeDir(override val path: String) extends PrepareFSAction {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <mkdir path={path} />
}
