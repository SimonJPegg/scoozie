package org.antipathy.scoozie.action.prepare

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}

class PrepareSpec extends FlatSpec with Matchers {

  behavior of "Prepare"

  it should "generate valid XML" in {
    val result =
      Prepare(Seq(Delete("/Some/Path"), MakeDir("/Some/Path"))).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<prepare>
      <delete path="/Some/Path"/>
      <mkdir path="/Some/Path"/>
    </prepare>))
  }

}
