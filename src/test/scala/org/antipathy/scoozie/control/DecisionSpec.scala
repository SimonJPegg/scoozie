package org.antipathy.scoozie.control

import org.antipathy.scoozie.action.Action
import org.antipathy.scoozie.Node
import org.scalatest.{FlatSpec, Matchers}
import scala.xml.Elem
import scala.xml
import scala.collection.immutable.Map

class DecisionSpec extends FlatSpec with Matchers {

  behavior of "Decision"

  it should "generate valid XML" in {

    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    val predicateValue = "${somePredicate}"

    val result = Decision(name = "SomeDecision",
                          default = oozieNode,
                          Switch(oozieNode, "somePredicate"),
                          Switch(oozieNode, "somePredicate")).action.toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<decision name="SomeDecision">
      <switch>
        <case to="SomeNode">{predicateValue}</case>
        <case to="SomeNode">{predicateValue}</case>
        <default to="SomeNode"/>
      </switch>
    </decision>))
  }
}
