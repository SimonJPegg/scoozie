package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class ArgsSpec extends FlatSpec with Matchers {

  behavior of "Args"

  it should "generate valid XML" in {
    val result =
      Args("someValue").toXML
    xml.Utility.trim(result) should be(
      xml.Utility.trim(<args>someValue</args>)
    )
  }

}
