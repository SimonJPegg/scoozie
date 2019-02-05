package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class EnvVarSpec extends FlatSpec with Matchers {

  behavior of "EnvVar"

  it should "generate valid XML" in {
    val result =
      EnvVar("someValue").toXML
    xml.Utility.trim(result) should be(xml.Utility.trim(<env-var>someValue</env-var>))
  }

}
