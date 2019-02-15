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
package org.antipathy.scoozie.functions

import org.scalatest.{FlatSpec, Matchers}

class BasicFunctionsSpec extends FlatSpec with Matchers {

  behavior of "BasicFunctions"

  it should "provide string representations of basic functions" in {

    BasicFunctions.firstNotNull("value1", "value2") should be(s"firstNotNull(value1,value2)")
    BasicFunctions.concat("s1", "s2") should be(s"concat(s1,s2)")
    BasicFunctions.replaceAll("src", "regex", "replacement") should be(s"replaceAll(src,regex,replacement)")
    BasicFunctions.appendAll("src", "append", "delimeter") should be(s"appendAll(src,append,delimeter)")
    BasicFunctions.trim("s") should be(s"trim(s)")
    BasicFunctions.urlEncode("s") should be(s"urlEncode(s)")
    BasicFunctions.timestamp should be("timestamp()")
    BasicFunctions.toJsonStr("variable") should be(s"toJsonStr(variable)")
    BasicFunctions.toPropertiesStr("variable") should be(s"toPropertiesStr(variable)")
    BasicFunctions.toConfigurationStr("variable") should be(s"toConfigurationStr(variable)")
  }

}
