package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}

class ArgSpec extends FlatSpec with Matchers {

  behavior of "Arg"

  it should "generate valid XML" in {
    val result =
      Arg("someValue").toXML
    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<arg>someValue</arg>))
  }

}
