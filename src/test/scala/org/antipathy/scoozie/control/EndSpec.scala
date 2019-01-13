package org.antipathy.scoozie.control
import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class EndSpec extends FlatSpec with Matchers {

  behavior of "End"

  it should "generate valid XML" in {

    val result = new End().toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<end name="end" />))
  }
}
