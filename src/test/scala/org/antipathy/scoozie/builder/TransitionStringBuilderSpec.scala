package org.antipathy.scoozie.builder

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class TransitionStringBuilderSpec extends FlatSpec with Matchers {

  behavior of "TransitionStringBuilder"

  it should "build the transition string for validation" in {

    val result =
      TransitionStringBuilder.build(ConfigFactory.parseString("""
                                                                | validate {
                                                                |   transitions = "start  -> sparkAction -> end"
                                                                | }
      """.stripMargin))

    result should be("start -> sparkAction -> end")
  }
}
