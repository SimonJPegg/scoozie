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
package org.antipathy.scoozie.action.control

import org.antipathy.scoozie.action.Node
import org.antipathy.scoozie.xml.XmlSerializable

import scala.xml.Elem

/**
  * Ooozie decision node switch
  *
  * @param node the node to switch to
  * @param predicate the predicate for switching to this node
  */
case class Switch(node: Node, predicate: String) extends XmlSerializable {

  /**
    * expected predicate pattern for oozie switches
    */
  private val Pattern = """[${].*[}]""".r

  /**
    * format predicates to expected pattern
    */
  private val formattedPredicate = predicate match {
    case Pattern() => predicate
    case _         => "${" + predicate + "}"
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <case to={node.action.name}>{formattedPredicate}</case>
}
