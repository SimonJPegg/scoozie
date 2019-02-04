package org.antipathy.scoozie.action

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration.{
  Configuration,
  Property,
  YarnConfig
}
import org.antipathy.scoozie.configuration.Credentials
import scala.xml
import scala.collection.immutable._

class ShellActionSpec extends FlatSpec with Matchers {

  behavior of "ShellAction"

  it should "generate valid XML with no config and no args" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             prepareOption = None,
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq(),
                             envVars = Seq(),
                             files = Seq(),
                             captureOutput = false,
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode")).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
        </shell>)
    )

    Map("${jobTracker}" -> "jobTracker",
        "${nameNode}" -> "nameNode",
        "${SomeAction_scriptName}" -> "script.sh",
        "${SomeAction_scriptLocation}" -> "/path/to/script.sh")
  }

  it should "generate valid XML with arguments" in {
    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             prepareOption = None,
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq("one", "two"),
                             envVars = Seq(),
                             captureOutput = false,
                             files = Seq(),
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode")).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <argument>{"${SomeAction_commandLineArgs0}"}</argument>
          <argument>{"${SomeAction_commandLineArgs1}"}</argument>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
        </shell>)
    )

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${SomeAction_commandLineArgs1}" -> "two",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_scriptLocation}" -> "/path/to/script.sh",
          "${SomeAction_scriptName}" -> "script.sh",
          "${SomeAction_commandLineArgs0}" -> "one")
    )
  }

  it should "generate valid XML with config" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             prepareOption = None,
                             files = Seq(),
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq(),
                             envVars = Seq(),
                             captureOutput = false,
                             config = YarnConfig(
                               jobTracker = "jobTracker",
                               nameNode = "nameNode",
                               configuration =
                                 Configuration(Seq(Property("name", "value")))
                             )).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <configuration>
            <property>
              <name>name</name>
              <value>{"${SomeAction_property0}"}</value>
            </property>
          </configuration>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
        </shell>)
    )

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_scriptLocation}" -> "/path/to/script.sh",
          "${SomeAction_scriptName}" -> "script.sh",
          "${SomeAction_property0}" -> "value")
    )
  }

  it should "generate valid XML with env vars" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             prepareOption = None,
                             files = Seq(),
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq("one", "two"),
                             envVars = Seq("user=me"),
                             captureOutput = false,
                             config = YarnConfig(
                               jobTracker = "jobTracker",
                               nameNode = "nameNode",
                               configuration =
                                 Configuration(Seq(Property("name", "value")))
                             )).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <configuration>
            <property>
              <name>name</name>
              <value>{"${SomeAction_property0}"}</value>
            </property>
          </configuration>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <argument>{"${SomeAction_commandLineArgs0}"}</argument>
          <argument>{"${SomeAction_commandLineArgs1}"}</argument>
          <env-var>{"${SomeAction_envVars0}"}</env-var>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
        </shell>)
    )

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${SomeAction_commandLineArgs1}" -> "two",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_scriptLocation}" -> "/path/to/script.sh",
          "${SomeAction_scriptName}" -> "script.sh",
          "${SomeAction_property0}" -> "value",
          "${SomeAction_envVars0}" -> "user=me",
          "${SomeAction_commandLineArgs0}" -> "one")
    )
  }

  it should "capture output when required" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             prepareOption = None,
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq(),
                             envVars = Seq(),
                             files = Seq(),
                             captureOutput = true,
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode")).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
          <capture-output/>
        </shell>)
    )

    result.properties should be(
      Map("${jobTracker}" -> "jobTracker",
          "${nameNode}" -> "nameNode",
          "${SomeAction_scriptName}" -> "script.sh",
          "${SomeAction_scriptLocation}" -> "/path/to/script.sh")
    )
  }

}