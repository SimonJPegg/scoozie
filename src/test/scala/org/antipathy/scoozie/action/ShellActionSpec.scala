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
package org.antipathy.scoozie.action

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.configuration.{Configuration, Credentials, Property, YarnConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class ShellActionSpec extends FlatSpec with Matchers {

  behavior of "ShellAction"

  it should "generate valid XML with no config and no args" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq(),
                             envVars = Seq(),
                             files = Seq(),
                             captureOutput = false,
                             jobXmlOption = None,
                             prepareOption = None,
                             configuration = Scoozie.Configuration.emptyConfig,
                             yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
        </shell>))

    Map("${jobTracker}" -> "jobTracker",
        "${nameNode}" -> "nameNode",
        "${SomeAction_scriptName}" -> "script.sh",
        "${SomeAction_scriptLocation}" -> "/path/to/script.sh")
  }

  it should "generate valid XML with arguments" in {
    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq("one", "two"),
                             envVars = Seq(),
                             captureOutput = false,
                             jobXmlOption = None,
                             prepareOption = None,
                             files = Seq(),
                             configuration = Scoozie.Configuration.emptyConfig,
                             yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <argument>{"${SomeAction_commandLineArgs0}"}</argument>
          <argument>{"${SomeAction_commandLineArgs1}"}</argument>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
        </shell>))

    result.properties should be(
      Map("${SomeAction_commandLineArgs1}" -> "two",
          "${SomeAction_scriptLocation}" -> "/path/to/script.sh",
          "${SomeAction_scriptName}" -> "script.sh",
          "${SomeAction_commandLineArgs0}" -> "one")
    )
  }

  it should "generate valid XML with config" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             files = Seq(),
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq(),
                             envVars = Seq(),
                             captureOutput = false,
                             jobXmlOption = None,
                             prepareOption = None,
                             configuration = Configuration(Seq(Property("name", "value"))),
                             yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
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
        </shell>))

    result.properties should be(
      Map("${SomeAction_scriptLocation}" -> "/path/to/script.sh",
          "${SomeAction_scriptName}" -> "script.sh",
          "${SomeAction_property0}" -> "value")
    )
  }

  it should "generate valid XML with env vars" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = ShellAction(name = "SomeAction",
                             files = Seq(),
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq("one", "two"),
                             envVars = Seq("user=me"),
                             captureOutput = false,
                             jobXmlOption = None,
                             prepareOption = None,
                             configuration = Configuration(Seq(Property("name", "value"))),
                             yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
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
        </shell>))

    result.properties should be(
      Map("${SomeAction_commandLineArgs1}" -> "two",
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
                             scriptName = "script.sh",
                             scriptLocation = "/path/to/script.sh",
                             commandLineArgs = Seq(),
                             envVars = Seq(),
                             files = Seq(),
                             captureOutput = true,
                             jobXmlOption = None,
                             prepareOption = None,
                             configuration = Scoozie.Configuration.emptyConfig,
                             yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<shell xmlns="uri:oozie:shell-action:0.1">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <exec>{"${SomeAction_scriptName}"}</exec>
          <file>{"${SomeAction_scriptLocation}#${SomeAction_scriptName}"}</file>
          <capture-output/>
        </shell>))

    result.properties should be(
      Map("${SomeAction_scriptName}" -> "script.sh", "${SomeAction_scriptLocation}" -> "/path/to/script.sh")
    )
  }

}
