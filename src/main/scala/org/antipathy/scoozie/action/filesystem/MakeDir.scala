package org.antipathy.scoozie.action.filesystem

import org.antipathy.scoozie.action.prepare.PrepareFSAction
import org.antipathy.scoozie.properties.PropertyFormatter

import scala.xml.Elem

/**
  * Oozie make directory definition
  *
  * @param path the path to create
  */
case class MakeDir(override val path: String) extends PrepareFSAction with FileSystemAction with PropertyFormatter {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <mkdir path={formatProperty(path)} />
}
