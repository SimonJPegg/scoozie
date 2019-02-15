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
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.Elem

class JoinSpec extends FlatSpec with Matchers {

  behavior of "Join"

  it should "generate valid XML" in {
    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    val result = Join("SomeJoin", oozieNode).action.toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<join name="SomeJoin" to="SomeNode" />))
  }

}
