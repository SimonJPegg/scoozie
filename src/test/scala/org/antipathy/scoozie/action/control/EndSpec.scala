package org.antipathy.scoozie.action.control
import org.scalatest.{FlatSpec, Matchers}

class EndSpec extends FlatSpec with Matchers {

  behavior of "End"

  it should "generate valid XML" in {

    val result = new End().toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<end name="end" />))
  }
}
