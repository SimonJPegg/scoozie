package org.antipathy.scoozie.action.prepare

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class DeleteSpec extends FlatSpec with Matchers {

  behavior of "Delete"

  it should "generate valid XML" in {
    val result = Delete("/Some/Path").toXML

    xml.Utility.trim(result) should be(
      xml.Utility.trim(<delete path="/Some/Path" />)
    )
  }

}
