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

import org.antipathy.scoozie.action.{Action, Node}

import scala.collection.immutable.Map
import scala.xml.Elem

/**
  * Oozie join control node
  * @param name the name of the join
  * @param to the node the join transitions to
  */
final class Join(override val name: String, to: Node) extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The element to transition to on success
    */
  def transitionTo: Node = to

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <join name={name} to={to.action.name} />
}

object Join {
  def apply(name: String, to: Node): Node =
    Node(new Join(name, to))(None)
}
