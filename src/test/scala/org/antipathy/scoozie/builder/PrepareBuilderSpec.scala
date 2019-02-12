package org.antipathy.scoozie.builder

import com.typesafe.config.ConfigFactory
import org.antipathy.scoozie.action.filesystem._
import org.scalatest.{FlatSpec, Matchers}

class PrepareBuilderSpec extends FlatSpec with Matchers {

  behavior of "PrepareBuilder"

  it should "build an empty option when no prepare config is supplied" in {
    val result = PrepareBuilder.build(ConfigFactory.empty())
    result.isDefined should be(false)
  }

  it should "build a prepare step from config" in {
    val result = PrepareBuilder.build(ConfigFactory.parseString("""
                                                                  |prepare: {
                                                                  |  delete: "deletePath"
                                                                  |  mkdir: "makePath"
                                                                  |}
      """.stripMargin))
    result.isDefined should be(true)
    result.foreach { item =>
      item.actions.length should be(2)
      item.actions.foreach {
        case Delete(path)  => path should be("\"deletePath\"")
        case MakeDir(path) => path should be("\"makePath\"")
        case _             => throw new IllegalArgumentException("something funky going on here")
      }
    }

  }
}
