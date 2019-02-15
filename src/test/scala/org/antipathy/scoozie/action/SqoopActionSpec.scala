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
import org.antipathy.scoozie.configuration.Credentials
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class SqoopActionSpec extends FlatSpec with Matchers {

  behavior of "SqoopAction"

  it should "generate valid XML with a command" in {

    implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials

    val result = Scoozie.Actions
      .sqoopAction(name = "sqoopAction",
                   command = "sqoopCommand",
                   files = Seq("one", "two"),
                   jobXmlOption = None,
                   configuration = Scoozie.Configuration.configuration(Map("key" -> "value")),
                   yarnConfig = Scoozie.Configuration.yarnConfig("someJT", "someNN"),
                   prepareOption = None)
      .action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<sqoop xmlns="uri:oozie:sqoop-action:0.3">
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <configuration>
          <property>
            <name>key</name>
            <value>{"${sqoopAction_property0}"}</value>
          </property>
        </configuration>
        <command>{"${sqoopAction_command}"}</command>
        <file>{"${sqoopAction_files0}"}</file>
        <file>{"${sqoopAction_files1}"}</file>
      </sqoop>))

    result.properties should be(
      Map("${sqoopAction_files0}" -> "one",
          "${sqoopAction_files1}" -> "two",
          "${sqoopAction_property0}" -> "value",
          "${sqoopAction_command}" -> "sqoopCommand")
    )
  }

  it should "generate valid XML with arguments" in {

    implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials

    val result = Scoozie.Actions
      .sqoopAction(name = "sqoopAction",
                   args = Seq("arg1", "arg2"),
                   files = Seq("one", "two"),
                   jobXmlOption = None,
                   configuration = Scoozie.Configuration.configuration(Map("key" -> "value")),
                   yarnConfig = Scoozie.Configuration.yarnConfig("someJT", "someNN"),
                   prepareOption = None)
      .action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<sqoop xmlns="uri:oozie:sqoop-action:0.3">
      <job-tracker>{"${jobTracker}"}</job-tracker>
      <name-node>{"${nameNode}"}</name-node>
      <configuration>
        <property>
          <name>key</name>
          <value>{"${sqoopAction_property0}"}</value>
        </property>
      </configuration>
      <arg>{"${sqoopAction_arguments0}"}</arg>
      <arg>{"${sqoopAction_arguments1}"}</arg>
      <file>{"${sqoopAction_files0}"}</file>
      <file>{"${sqoopAction_files1}"}</file>
    </sqoop>))

    result.properties should be(
      Map("${sqoopAction_files1}" -> "two",
          "${sqoopAction_arguments0}" -> "arg1",
          "${sqoopAction_arguments1}" -> "arg2",
          "${sqoopAction_property0}" -> "value",
          "${sqoopAction_files0}" -> "one")
    )
  }

}
