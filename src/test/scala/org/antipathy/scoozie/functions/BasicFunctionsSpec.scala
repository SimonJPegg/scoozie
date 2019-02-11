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
