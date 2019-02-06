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
