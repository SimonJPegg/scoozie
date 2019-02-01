package org.antipathy.scoozie.control

import org.antipathy.scoozie.action.Action
import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.Node
import scala.xml.Elem
import scala.xml

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

    xml.Utility.trim(result) should be(
      xml.Utility.trim(<join name="SomeJoin" to="SomeNode" />)
    )
  }

}
