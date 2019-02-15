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
  * oozie Start control node
  */
final class Start extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The name of this element
    */
  override def name: String = "start"
  // $COVERAGE-OFF$
  /**
    * The XML for this node
    */
  override def toXML: Elem = <unused />
  // $COVERAGE-ON$
}

object Start {

  def apply(): Node = Node(new Start())(None)
}
