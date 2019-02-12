package org.antipathy.scoozie.action.control

import org.antipathy.scoozie.action.{Action, Node}
import org.antipathy.scoozie.exception.TransitionException
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.Elem

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

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<fork name="SomeFork">
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

    an[TransitionException] should be thrownBy {
      Fork("SomeFork", Seq(oozieNode)).toXML
    }
  }
}
