package org.antipathy.scoozie.action.filesystem

import org.antipathy.scoozie.properties.PropertyFormatter

import scala.xml.Elem

/**
  * Create an ozzie move step
  * @param srcPath the path to move from
  * @param targetPath the path to move to
  */
case class Move(srcPath: String, targetPath: String) extends FileSystemAction with PropertyFormatter {

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <move source={formatProperty(srcPath)} target={formatProperty(targetPath)} />
}
