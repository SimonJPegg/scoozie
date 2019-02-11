package org.antipathy.scoozie.configuration
import org.scalatest.{FlatSpec, Matchers}

class ParamSpec extends FlatSpec with Matchers {

  behavior of "Param"

  it should "generate valid XML" in {
    val result =
      Param("someValue").toXML
    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<param>someValue</param>))
  }

}
