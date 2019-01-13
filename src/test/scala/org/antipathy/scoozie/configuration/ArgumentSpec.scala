package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class ArgumentSpec extends FlatSpec with Matchers {

  behavior of "Argument"

  it should "generate valid XML" in {
    val result =
      Argument("someValue").toXML
    xml.Utility.trim(result) should be(
      xml.Utility.trim(<argument>someValue</argument>)
    )
  }

}
