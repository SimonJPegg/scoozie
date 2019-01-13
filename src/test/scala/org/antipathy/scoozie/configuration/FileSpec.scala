package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class FileSpec extends FlatSpec with Matchers {

  behavior of "File"

  it should "generate valid XML" in {
    val result =
      File("someValue").toXML
    xml.Utility.trim(result) should be(
      xml.Utility.trim(<file>someValue</file>)
    )
  }

}
