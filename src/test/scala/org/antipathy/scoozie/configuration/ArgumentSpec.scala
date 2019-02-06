package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}

class ArgumentSpec extends FlatSpec with Matchers {

  behavior of "Argument"

  it should "generate valid XML" in {
    val result =
      Argument("someValue").toXML
    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<argument>someValue</argument>))
  }

}
