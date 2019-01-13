package org.antipathy.scoozie.action

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration.YarnConfig
import org.antipathy.scoozie.configuration.Credentials
import scala.xml
import scala.collection.immutable._

class Hive2ActionSpec extends FlatSpec with Matchers {

  behavior of "Hive2Action"

  it should "generate valid XML with no config and no parameters" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = Hive2Action(name = "hive2Action",
                             hiveSettingsXML = "hiveSettingsXML",
                             scriptName = "scriptName.hql",
                             scriptLocation = "/path/to/criptName.hql",
                             parameters = Seq("one", "two"),
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode"),
                             jdbcUrl = "jdbcUrl").action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<hive2 xmlns="uri:oozie:hive2-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${hive2Action_hiveSettingsXML}"}</job-xml>
          <jdbc-url>{"${hive2Action_jdbcUrl}"}</jdbc-url>
          <script>{"${hive2Action_scriptName}"}</script>
          <param>{"${hive2Action_parameter0}"}</param>
          <param>{"${hive2Action_parameter1}"}</param>
          <file>{"${hive2Action_scriptLocation}"}</file>
        </hive2>)
    )

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${jobTracker}" -> "jobTracker",
          "${hive2Action_hiveSettingsXML}" -> "hiveSettingsXML",
          "${hive2Action_scriptLocation}" -> "/path/to/criptName.hql",
          "${hive2Action_scriptName}" -> "scriptName.hql",
          "${hive2Action_jdbcUrl}" -> "jdbcUrl")
    )
  }
}
