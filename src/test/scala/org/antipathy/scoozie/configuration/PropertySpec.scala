package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}

class PropertySpec extends FlatSpec with Matchers {

  behavior of "Property"

  it should "generate valid XML" in {
    val result =
      Property("someProp", "someValue").toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<property>
        <name>someProp</name>
        <value>someValue</value>
      </property>))
  }

}
