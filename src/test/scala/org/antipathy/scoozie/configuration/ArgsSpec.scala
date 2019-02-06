package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}

class ArgsSpec extends FlatSpec with Matchers {

  behavior of "Args"

  it should "generate valid XML" in {
    val result =
      Args("someValue").toXML
    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<args>someValue</args>))
  }

}
