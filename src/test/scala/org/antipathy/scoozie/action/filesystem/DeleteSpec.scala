package org.antipathy.scoozie.action.filesystem

import org.scalatest.{FlatSpec, Matchers}

class DeleteSpec extends FlatSpec with Matchers {

  behavior of "Delete"

  it should "generate valid XML" in {
    val result = Delete("/Some/Path").toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<delete path="/Some/Path" />))
  }

}
