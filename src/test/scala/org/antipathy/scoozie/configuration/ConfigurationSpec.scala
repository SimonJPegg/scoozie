package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class ConfigurationSpec extends FlatSpec with Matchers {

  behavior of "Configuration"

  it should "generate valid XML" in {
    val result = Configuration(
      Seq(Property("mapred.compress.map.output", "true"),
          Property("oozie.hive.defaults", "/usr/foo/hive-0.6-default.xml"))
    ).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<configuration>
      <property>
        <name>mapred.compress.map.output</name>
        <value>true</value>
      </property>
      <property>
        <name>oozie.hive.defaults</name>
        <value>/usr/foo/hive-0.6-default.xml</value>
      </property>
    </configuration>))
  }

}
