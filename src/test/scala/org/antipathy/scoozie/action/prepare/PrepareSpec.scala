package org.antipathy.scoozie.action.prepare

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class PrepareSpec extends FlatSpec with Matchers {

  behavior of "Prepare"

  it should "generate valid XML" in {
    val result =
      Prepare(Seq(Delete("/Some/Path"), MakeDir("/Some/Path"))).toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<prepare>
      <delete path="/Some/Path"/>
      <mkdir path="/Some/Path"/>
    </prepare>))
  }

}
