package org.antipathy.scoozie.control

import org.scalatest.{FlatSpec, Matchers}
import scala.xml

class KillSpec extends FlatSpec with Matchers {

  behavior of "Join"

  it should "generate valid XML" in {
    xml.Utility.trim(Kill("Killed!").toXML) should be(xml.Utility.trim(<kill name="kill">
      <message>Killed!</message>
    </kill>))
  }

}
