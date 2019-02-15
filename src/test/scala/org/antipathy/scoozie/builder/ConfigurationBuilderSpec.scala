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
import org.antipathy.scoozie.exception.ConfigurationMissingException
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class ConfigurationBuilderSpec extends FlatSpec with Matchers {

  behavior of "ConfigurationBuilder"

  it should "build a configuration object" in {

    val result = ConfigurationBuilder.buildConfiguration(ConfigFactory.parseString("""configuration: {
                                                                                     |      workflow1:"value1",
                                                                                     |      workflow2:"value2",
                                                                                     |      workflow3:"value3",
                                                                                     |      workflow4:"value4"
                                                                                     |    }""".stripMargin))

    result.properties should be(
      Map("workflow1" -> "\"value1\"",
          "workflow2" -> "\"value2\"",
          "workflow3" -> "\"value3\"",
          "workflow4" -> "\"value4\"")
    )
  }

  it should "return an empty configuration when no config is supplied" in {
    val result = ConfigurationBuilder.buildConfiguration(ConfigFactory.empty())
    result.properties should be(Map())
  }

  it should "return an empty configuration when an empty config is supplied" in {
    val result = ConfigurationBuilder.buildConfiguration(ConfigFactory.parseString("configuration: {}"))
    result.properties should be(Map())
  }

  it should "build a credentials object" in {

    val result = ConfigurationBuilder.buildCredentials(ConfigFactory.parseString("""credentials {
                                                                                   |      name: "someCredentials"
                                                                                   |      type: "credentialsType"
                                                                                   |      configuration: {
                                                                                   |        credentials1:"value1",
                                                                                   |        credentials2:"value2",
                                                                                   |        credentials3:"value3",
                                                                                   |        credentials4:"value4"
                                                                                   |      }
                                                                                   |    }""".stripMargin))

    result.isDefined should be(true)
    result.foreach { item =>
      item.credential.name should be("someCredentials")
      item.credential.credentialsType should be("credentialsType")
      item.properties should be(
        Map("credentials1" -> "\"value1\"",
            "credentials2" -> "\"value2\"",
            "credentials3" -> "\"value3\"",
            "credentials4" -> "\"value4\"")
      )
    }

  }

  it should "build an empty credentials option when no credentials are supplied" in {
    val result = ConfigurationBuilder.buildCredentials(ConfigFactory.empty())
    result.isDefined should be(false)
  }

  it should "raise an error when a required property is missing" in {

    an[ConfigurationMissingException] should be thrownBy {
      ConfigurationBuilder.buildCredentials(ConfigFactory.parseString("""credentials {
                                                                        |      name: "someCredentials"
                                                                        |      configuration: {
                                                                        |        credentials1:"value1",
                                                                        |        credentials2:"value2",
                                                                        |        credentials3:"value3",
                                                                        |        credentials4:"value4"
                                                                        |      }
                                                                        |    }""".stripMargin))
    }

  }

}
