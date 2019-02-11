package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.xml.XmlSerializable

import scala.xml.Elem

/**
  * Oozie property definition
  *
  * @param name the name of the property
  * @param value the value of the property
  */
case class Property(name: String, value: String) extends XmlSerializable {

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <property>
      <name>{name}</name>
      <value>{value}</value>
    </property>
}
