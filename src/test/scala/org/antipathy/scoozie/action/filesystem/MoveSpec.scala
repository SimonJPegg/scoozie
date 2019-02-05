package org.antipathy.scoozie.action.filesystem

import scala.xml
import org.scalatest.{FlatSpec, Matchers}

class MoveSpec extends FlatSpec with Matchers {

  behavior of "Move"

  it should "generate valid XML" in {
    val result = Move(srcPath = "/Some/Path", targetPath = "/some/other/path").toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<move source="/Some/Path" target="/some/other/path" />))
  }

}
