package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}

class FileSpec extends FlatSpec with Matchers {

  behavior of "File"

  it should "generate valid XML" in {
    val result =
      File("someValue").toXML
    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<file>someValue</file>))
  }

}
