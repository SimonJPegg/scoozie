package org.antipathy.scoozie.action.filesystem

import org.scalatest.{FlatSpec, Matchers}

class MoveSpec extends FlatSpec with Matchers {

  behavior of "Move"

  it should "generate valid XML" in {
    val result = Move(srcPath = "/Some/Path", targetPath = "/some/other/path").toXML

    scala.xml.Utility.trim(result) should be(
      scala.xml.Utility.trim(<move source="/Some/Path" target="/some/other/path" />)
    )
  }

}
