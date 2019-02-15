/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
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
