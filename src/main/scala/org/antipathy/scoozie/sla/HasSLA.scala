// $COVERAGE-OFF$
package org.antipathy.scoozie.sla

import org.antipathy.scoozie.action.Nameable
import org.antipathy.scoozie.configuration.ActionProperties
import scala.xml.Elem

/**
  * Trait for building SLA properties
  */
trait HasSLA extends Nameable {

  def slaOption: Option[OozieSLA]

  //map the prepare step for this action
  protected val slaOptionAndProps: Option[ActionProperties[OozieSLA]] = slaOption.map(_.withActionName(name))
  protected val slaProperties: Map[String, String] =
    slaOptionAndProps.map(_.properties).getOrElse(Map[String, String]())
  protected val slaOptionMapped: Option[OozieSLA] = slaOptionAndProps.map(_.mappedType)

  /**
    * Render the XML for this SLA
    */
  protected def slaXML: Elem = slaOptionMapped.map(_.toXML).orNull
}
// $COVERAGE-ON$
