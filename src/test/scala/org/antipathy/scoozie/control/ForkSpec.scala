package org.antipathy.scoozie.control

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.action.Action
import scala.xml.Elem
import org.antipathy.scoozie.Node
import scala.xml

class ForkSpec extends FlatSpec with Matchers {
  behavior of "Fork"

  it should "generate valid XML" in {

    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    val result = Fork("SomeFork", Seq(oozieNode, oozieNode)).toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<fork name="SomeFork">
      <path start="SomeNode"/>
      <path start="SomeNode"/>
    </fork>))
  }

  it should "raise an error when insufficent paths are provided" in {
    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    an[IllegalArgumentException] should be thrownBy {
      Fork("SomeFork", Seq(oozieNode)).toXML
    }
  }
}
