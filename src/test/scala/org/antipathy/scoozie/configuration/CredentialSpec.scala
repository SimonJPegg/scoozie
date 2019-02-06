package org.antipathy.scoozie.configuration

import org.scalatest.{FlatSpec, Matchers}
import scala.collection.immutable._

class CredentialSpec extends FlatSpec with Matchers {

  behavior of "Credential"

  it should "generate valid XML" in {
    val result = Credential(name = "hive-credentials",
                            credentialsType = "hive",
                            properties = Seq(Property("hive2.jdbc.url", "jdbc:hive2://hiveserver2;ssl=true;"))).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<credential name="hive-credentials" type="hive">
        <property>
          <name>hive2.jdbc.url</name>
          <value>jdbc:hive2://hiveserver2;ssl=true;</value>
        </property>
      </credential>))
  }

}
