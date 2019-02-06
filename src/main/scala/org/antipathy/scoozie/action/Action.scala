// $COVERAGE-OFF$
package org.antipathy.scoozie.action

import org.antipathy.scoozie.properties.OozieProperties
import org.antipathy.scoozie.xml.XmlSerializable

/**
  * Base trait for oozie actions
  */
private[scoozie] trait Action extends XmlSerializable with Nameable with OozieProperties {

  /**
    * The XML namespace for an action element
    */
  def xmlns: Option[String]
}
// $COVERAGE-ON$
