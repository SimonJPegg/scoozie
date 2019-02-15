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
package org.antipathy.scoozie.action.prepare

import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class PrepareSpec extends FlatSpec with Matchers {

  behavior of "Prepare"

  it should "generate valid XML" in {
    val result =
      Prepare(Seq(Delete("/Some/Path"), MakeDir("/Some/Path"))).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<prepare>
      <delete path="${/Some/Path}"/>
      <mkdir path="${/Some/Path}"/>
    </prepare>))
  }

}
