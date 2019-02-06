package org.antipathy.scoozie.action.filesystem

import org.scalatest.{FlatSpec, Matchers}

class ChmodSpec extends FlatSpec with Matchers {

  behavior of "Chmod"

  it should "generate valid XML" in {
    val result = Chmod(path = "/Some/Path", permissions = "755", dirFiles = "false").toXML

    scala.xml.Utility.trim(result) should be(
      scala.xml.Utility.trim(<chmod path="/Some/Path" permissions="755" dir-files="false" />)
    )
  }

}
