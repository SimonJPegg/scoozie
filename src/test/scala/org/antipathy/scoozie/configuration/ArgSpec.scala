package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class ArgSpec extends FlatSpec with Matchers {

  behavior of "Arg"

  it should "generate valid XML" in {
    val result =
      Arg("someValue").toXML
    xml.Utility.trim(result) should be(xml.Utility.trim(<arg>someValue</arg>))
  }

}
