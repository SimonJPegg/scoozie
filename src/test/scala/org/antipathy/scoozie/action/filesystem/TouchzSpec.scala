package org.antipathy.scoozie.action.filesystem

import scala.xml
import org.scalatest.{FlatSpec, Matchers}

class TouchzSpec extends FlatSpec with Matchers {

  behavior of "Touchz"

  it should "generate valid XML" in {
    val result = Touchz("/Some/Path").toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<touchz path="/Some/Path" />))
  }

}
