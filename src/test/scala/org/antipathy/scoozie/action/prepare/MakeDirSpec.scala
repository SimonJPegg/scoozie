package org.antipathy.scoozie.action.prepare

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class MakeDirSpec extends FlatSpec with Matchers {

  behavior of "MakeDir"

  it should "generate valid XML" in {
    val result = MakeDir("/Some/Path").toXML

    xml.Utility.trim(result) should be(
      xml.Utility.trim(<mkdir path="/Some/Path" />)
    )
  }

}
