package org.antipathy.scoozie.builder

import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.ConfigFactory
import scala.collection.immutable._
import org.antipathy.scoozie.exception.ConfigurationMissingException

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
    result.get.credential.name should be("someCredentials")
    result.get.credential.credentialsType should be("credentialsType")
    result.get.properties should be(
      Map("credentials1" -> "\"value1\"",
          "credentials2" -> "\"value2\"",
          "credentials3" -> "\"value3\"",
          "credentials4" -> "\"value4\"")
    )
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
