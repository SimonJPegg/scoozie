package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}

class EnvVarSpec extends FlatSpec with Matchers {

  behavior of "EnvVar"

  it should "generate valid XML" in {
    val result =
      EnvVar("someValue").toXML
    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<env-var>someValue</env-var>))
  }

}
