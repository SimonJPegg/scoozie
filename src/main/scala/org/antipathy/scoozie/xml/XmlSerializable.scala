// $COVERAGE-OFF$
package org.antipathy.scoozie.xml

import scala.xml.Elem

/**
  * Base trait serialisation to XML
  */
private[scoozie] trait XmlSerializable {

  /**
    * The XML for this node
    */
  def toXML: Elem
}
// $COVERAGE-ON$
