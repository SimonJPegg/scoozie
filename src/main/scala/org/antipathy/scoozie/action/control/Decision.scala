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

import scala.collection.immutable
import scala.collection.immutable._
import scala.xml.Elem

/**
  * Oozie decision control node
  * @param name the name of the decision node
  * @param switches the switches in the decision node
  * @param default the default action for the decision
  */
final class Decision(override val name: String, default: Node, switches: Seq[Switch]) extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The default path for this node
    */
  def defaultPath: Node = default

  /**
    * The nodes contained within this fork
    */
  def transitionPaths: Seq[Node] = switches.map(_.node) :+ default

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <decision name={name}>
      <switch>
        {switches.map(_.toXML)}
        <default to={default.action.name}/>
      </switch>
    </decision>
}

object Decision {

  def apply(name: String, default: Node, switch: Switch*): Node =
    Node(new Decision(name, default, immutable.Seq(switch.toSeq: _*)))(None)

  def apply(name: String, default: Node, switches: Seq[Switch]): Node =
    Node(new Decision(name, default, switches))(None)
}
