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
package org.antipathy.scoozie.action.prepare

import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}
import org.antipathy.scoozie.configuration.ActionProperties
import org.antipathy.scoozie.xml.XmlSerializable

import scala.collection.immutable._
import scala.xml.Elem

/**
  * Ooize actions prepare definition
  * @param actions the prepare actions
  */
case class Prepare(actions: Seq[PrepareFSAction]) extends XmlSerializable {

  /**
    * Copy this action substituting the values for property names
    * @param actionName the name of the action calling this method
    * @return a copy of the action and its properties
    */
  private[scoozie] def withActionProperties(actionName: String): ActionProperties[Prepare] = {
    val mappedProps = actions.map {
      case d: Delete =>
        val p = Prepare.varPrefix + s"${actionName}_prepare_delete" + Prepare.varPostfix
        ActionProperties[PrepareFSAction](Delete(p), Map(p -> removeQuotes(d.path)))
      case m: MakeDir =>
        val p = Prepare.varPrefix + s"${actionName}_prepare_makedir" + Prepare.varPostfix
        ActionProperties[PrepareFSAction](MakeDir(p), Map(p -> removeQuotes(m.path)))
      case unknown =>
        throw new IllegalArgumentException(s"${unknown.getClass.getSimpleName} is not a valid prepare step")
    }
    ActionProperties(this.copy(mappedProps.map(_.mappedType)), mappedProps.flatMap(_.properties).toMap)
  }

  /**
    * remove any quotes in the passed in string
    */
  private def removeQuotes(s: String) = s.replace("\"", "")

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <prepare>
      {actions.map(_.toXML)}
    </prepare>
}

object Prepare {
  val varPrefix: String = "${"
  val varPostfix: String = "}"
}
