package org.antipathy.scoozie.action.filesystem

import org.antipathy.scoozie.properties.PropertyFormatter

import scala.xml.Elem

/**
  * Create an ozzie chmod step
  *
  * @param path the path to operate on
  * @param permissions the permissions to set
  * @param dirFiles should the operation be recursive
  */
case class Chmod(path: String, permissions: String, dirFiles: String) extends FileSystemAction with PropertyFormatter {

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <chmod path={formatProperty(path)} permissions={formatProperty(permissions)} dir-files={formatProperty(dirFiles)} />
}
