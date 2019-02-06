package org.antipathy.scoozie.action.control

import org.scalatest.{FlatSpec, Matchers}

class KillSpec extends FlatSpec with Matchers {

  behavior of "Join"

  it should "generate valid XML" in {
    scala.xml.Utility.trim(Kill("Killed!").toXML) should be(scala.xml.Utility.trim(<kill name="kill">
      <message>Killed!</message>
    </kill>))
  }

}
