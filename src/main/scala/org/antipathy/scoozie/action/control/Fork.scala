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
import org.antipathy.scoozie.exception.TransitionException

import scala.collection.immutable.Map
import scala.xml.Elem

/**
  * Oozie Fork control node
  * @param name the name of the fork
  * @param nodes the nodes within the fork
  */
final class Fork(override val name: String, nodes: Seq[Node]) extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The nodes contained within this fork
    */
  def transitionPaths: Seq[Node] = nodes

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem = {
    if (transitionPaths.length < 2) {
      throw new TransitionException(s"Error in Fork($name): must have at least 2 actions")
    }

    val pathsXml = transitionPaths.map(n => <path start={n.action.name} />)
    <fork name={name}>
        {pathsXml}
      </fork>
  }
}

object Fork {
  def apply(name: String, nodes: Seq[Node]): Node =
    Node(new Fork(name, nodes))(None)
}
