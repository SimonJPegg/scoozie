package org.antipathy.scoozie.action

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration.{Configuration, Property, YarnConfig}
import org.antipathy.scoozie.configuration.Credentials
import scala.collection.immutable._
import org.antipathy.scoozie.Scoozie

class HiveActionSpec extends FlatSpec with Matchers {

  behavior of "HiveAction"

  it should "generate valid XML with no config and no parameters" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = HiveAction(name = "SomeAction",
                            scriptName = "someScript.hql",
                            scriptLocation = "/path/to/someScript.hql",
                            parameters = Seq(),
                            jobXmlOption = Some("/path/to/settings.xml"),
                            files = Seq(),
                            prepareOption = None,
                            configuration = Scoozie.Configuration.emptyConfiguration,
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<hive xmlns="uri:oozie:hive-action:0.2">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${SomeAction_jobXML}"}</job-xml>
          <script>{"${SomeAction_scriptName}"}</script>
          <file>{"${SomeAction_scriptLocation}"}</file>
        </hive>))

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_scriptLocation}" -> "/path/to/someScript.hql",
          "${SomeAction_scriptName}" -> "someScript.hql",
          "${SomeAction_jobXML}" -> "/path/to/settings.xml")
    )
  }

  it should "generate valid XML with parameters" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = HiveAction(name = "SomeAction",
                            jobXmlOption = Some("/path/to/settings.xml"),
                            files = Seq(),
                            scriptName = "someScript.hql",
                            scriptLocation = "/path/to/someScript.hql",
                            parameters = Seq("tableName=\"SomeTable\"", "date=\"2019-01-13\""),
                            prepareOption = None,
                            configuration = Scoozie.Configuration.emptyConfiguration,
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<hive xmlns="uri:oozie:hive-action:0.2">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${SomeAction_jobXML}"}</job-xml>
          <script>{"${SomeAction_scriptName}"}</script>
          <param>{"${SomeAction_parameter0}"}</param>
          <param>{"${SomeAction_parameter1}"}</param>
          <file>{"${SomeAction_scriptLocation}"}</file>
        </hive>))

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_parameter0}" -> "tableName=\"SomeTable\"",
          "${SomeAction_scriptLocation}" -> "/path/to/someScript.hql",
          "${SomeAction_scriptName}" -> "someScript.hql",
          "${SomeAction_parameter1}" -> "date=\"2019-01-13\"",
          "${SomeAction_jobXML}" -> "/path/to/settings.xml")
    )
  }

  it should "generate valid XML with config" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = HiveAction(name = "SomeAction",
                            jobXmlOption = Some("/path/to/settings.xml"),
                            files = Seq(),
                            scriptName = "someScript.hql",
                            scriptLocation = "/path/to/someScript.hql",
                            parameters = Seq(),
                            prepareOption = None,
                            configuration = Configuration(
                              Seq(Property("someProp1", "someValue2"), Property("someProp2", "someValue2"))
                            ),
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<hive xmlns="uri:oozie:hive-action:0.2">
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <job-xml>{"${SomeAction_jobXML}"}</job-xml>
            <configuration>
              <property>
                <name>someProp1</name>
                <value>{"${SomeAction_property0}"}</value>
              </property>
              <property>
                <name>someProp2</name>
                <value>{"${SomeAction_property1}"}</value>
              </property>
            </configuration>
            <script>{"${SomeAction_scriptName}"}</script>
            <file>{"${SomeAction_scriptLocation}"}</file>
          </hive>))

    result.properties should be(
      Map("${SomeAction_property1}" -> "someValue2",
          "${nameNode}" -> "nameNode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_scriptLocation}" -> "/path/to/someScript.hql",
          "${SomeAction_scriptName}" -> "someScript.hql",
          "${SomeAction_jobXML}" -> "/path/to/settings.xml",
          "${SomeAction_property0}" -> "someValue2")
    )
  }

  it should "generate valid XML with config and parameters" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = HiveAction(name = "SomeAction",
                            jobXmlOption = Some("/path/to/settings.xml"),
                            files = Seq(),
                            scriptName = "someScript.hql",
                            scriptLocation = "/path/to/someScript.hql",
                            parameters = Seq("tableName=\"SomeTable\"", "date=\"2019-01-13\""),
                            prepareOption = None,
                            configuration = Configuration(
                              Seq(Property("someProp1", "someValue2"), Property("someProp2", "someValue2"))
                            ),
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<hive xmlns="uri:oozie:hive-action:0.2">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${SomeAction_jobXML}"}</job-xml>
          <configuration>
            <property>
              <name>someProp1</name>
              <value>{"${SomeAction_property0}"}</value>
            </property>
            <property>
              <name>someProp2</name>
              <value>{"${SomeAction_property1}"}</value>
            </property>
          </configuration>
          <script>{"${SomeAction_scriptName}"}</script>
          <param>{"${SomeAction_parameter0}"}</param>
          <param>{"${SomeAction_parameter1}"}</param>
          <file>{"${SomeAction_scriptLocation}"}</file>
        </hive>))

    result.properties should be(
      Map("${SomeAction_property1}" -> "someValue2",
          "${nameNode}" -> "nameNode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_parameter0}" -> "tableName=\"SomeTable\"",
          "${SomeAction_scriptLocation}" -> "/path/to/someScript.hql",
          "${SomeAction_scriptName}" -> "someScript.hql",
          "${SomeAction_parameter1}" -> "date=\"2019-01-13\"",
          "${SomeAction_jobXML}" -> "/path/to/settings.xml",
          "${SomeAction_property0}" -> "someValue2")
    )
  }
}
