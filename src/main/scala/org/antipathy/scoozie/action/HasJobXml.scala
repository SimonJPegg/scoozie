// $COVERAGE-OFF$
package org.antipathy.scoozie.action

import org.antipathy.scoozie.properties.OozieProperties
import scala.xml.Elem

/**
  * Trait for building prepare properties
  */
trait HasJobXml {
  this: Nameable with OozieProperties =>

  def jobXmlOption: Option[String]

  //map the jobXML for this action
  protected val jobXmlProperty: Map[_ <: String, String] = buildStringOptionProperty(name, "jobXml", jobXmlOption)

  /**
    * Render the XML for this element
    */
  protected def jobXml: Elem = jobXmlProperty.keys.map(k => <job-xml>{k}</job-xml>).headOption.orNull
}
// $COVERAGE-ON$
