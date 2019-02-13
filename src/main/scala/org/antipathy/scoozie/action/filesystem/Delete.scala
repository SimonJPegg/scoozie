package org.antipathy.scoozie.action.filesystem

import org.antipathy.scoozie.action.prepare.PrepareFSAction
import org.antipathy.scoozie.properties.PropertyFormatter

import scala.xml.Elem

/**
  * Oozie delete definition
  *
  * @param path the path to delete
  */
case class Delete(override val path: String) extends PrepareFSAction with FileSystemAction with PropertyFormatter {

  /**
    * The XML for this node
    */
  override def toXML: Elem = <delete path={formatProperty(path)} />
}
