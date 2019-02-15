// $COVERAGE-OFF$
/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
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
