package org.antipathy.scoozie.xml

/**
  * Base trait serialisation to XML
  */
private[scoozie] trait XmlSerializable {
  import scala.xml.Elem

  /**
    * The XML for this node
    */
  def toXML: Elem
}
