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
package org.antipathy.scoozie.action
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.ActionProperties

import scala.collection.immutable.Map
import scala.xml.Elem

/**
  * Trait for building prepare properties
  */
private[scoozie] trait HasPrepare {
  this: Nameable =>

  def prepareOption: Option[Prepare]

  //map the prepare step for this action
  protected val prepareOptionAndProps: Option[ActionProperties[Prepare]] =
    prepareOption.map(_.withActionProperties(name))
  protected val prepareProperties: Map[String, String] =
    prepareOptionAndProps.map(_.properties).getOrElse(Map[String, String]())
  protected val prepareOptionMapped: Option[Prepare] = prepareOptionAndProps.map(_.mappedType)

  /**
    * Render the XML for this prepare step
    */
  protected def prepareXML: Elem = prepareOptionMapped.map(_.toXML).orNull
}
// $COVERAGE-ON$
