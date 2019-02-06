package org.antipathy.scoozie.action.filesystem

import org.scalatest.{FlatSpec, Matchers}

class TouchzSpec extends FlatSpec with Matchers {

  behavior of "Touchz"

  it should "generate valid XML" in {
    val result = Touchz("/Some/Path").toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<touchz path="/Some/Path" />))
  }

}
