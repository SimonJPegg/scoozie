package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class PropertySpec extends FlatSpec with Matchers {

  behavior of "Property"

  it should "generate valid XML" in {
    val result =
      Property("someProp", "someValue").toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<property>
        <name>someProp</name>
        <value>someValue</value>
      </property>))
  }

}
