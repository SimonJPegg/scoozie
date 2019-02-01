package org.antipathy.scoozie.control

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.Node
import org.antipathy.scoozie.action.Action
import scala.xml
import scala.xml.Elem

class SwitchSpec extends FlatSpec with Matchers {

  behavior of "Switch"

  it should "generate valid XML" in {

    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    val predicateValue = "${SomePredicate}"
    val result = Switch(oozieNode, "SomePredicate").toXML

    xml.Utility.trim(result) should be(
      xml.Utility.trim(<case to="SomeNode">{predicateValue}</case>)
    )
  }

}
