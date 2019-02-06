package org.antipathy.scoozie.action.filesystem

import org.scalatest.{FlatSpec, Matchers}

class MakeDirSpec extends FlatSpec with Matchers {

  behavior of "MakeDir"

  it should "generate valid XML" in {
    val result = MakeDir("/Some/Path").toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<mkdir path="/Some/Path" />))
  }

}
