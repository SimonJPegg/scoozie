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
package org.antipathy.scoozie.coordinator

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.action.control.Start
import org.antipathy.scoozie.configuration.{Configuration, Credentials, Property, YarnConfig}
import org.antipathy.scoozie.workflow.Workflow
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class CoordinatorSpec extends FlatSpec with Matchers {

  behavior of "CoOrdinator"

  it should "generate valid XML without config" in {

    implicit val credentialsOption: Option[Credentials] = None

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "/path/to/workflow.xml",
                            transitions = Start(),
                            jobXmlOption = None,
                            configuration = Scoozie.Configuration.emptyConfig,
                            yarnConfig = YarnConfig(jobTracker = "", nameNode = ""))

    val result = Coordinator(name = "SomeCoOrd",
                             path = "somePath",
                             frequency = "${coord:days(1)}",
                             start = "2009-01-02T08:00Z",
                             end = "2009-01-04T08:00Z",
                             timezone = "America/Los_Angeles",
                             workflow = workflow,
                             configuration = Scoozie.Configuration.emptyConfig).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<coordinator-app
        name="SomeCoOrd"
        frequency="${SomeCoOrd_frequency}"
        start="${SomeCoOrd_start}"
        end="${SomeCoOrd_end}"
        timezone="${SomeCoOrd_timezone}"
        xmlns="uri:oozie:coordinator:0.1">
        <action>
          <workflow>
            <app-path>{"${SomeCoOrd_workflow_path}"}</app-path>
          </workflow>
        </action>
      </coordinator-app>))
  }

  it should "generate valid XML with config" in {

    implicit val credentialsOption: Option[Credentials] = None

    val functionVal = "${coord:nominalTime()}"

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "/path/to/workflow.xml",
                            transitions = Start(),
                            jobXmlOption = None,
                            configuration = Configuration(Seq(Property("some", "value"))),
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "Namenode"))

    val result = Coordinator(name = "SomeCoOrd",
                             path = "somePath",
                             frequency = "${coord:days(1)}",
                             start = "2009-01-02T08:00Z",
                             end = "2009-01-04T08:00Z",
                             timezone = "America/Los_Angeles",
                             workflow = workflow,
                             configuration =
                               Configuration(Seq(Property("some", "value"), Property("nominalTime", functionVal))))

    result.jobProperties should be("""SomeCoOrd_end=2009-01-04T08:00Z
                                     |SomeCoOrd_frequency=${coord:days(1)}
                                     |SomeCoOrd_property0=value
                                     |SomeCoOrd_start=2009-01-02T08:00Z
                                     |SomeCoOrd_timezone=America/Los_Angeles
                                     |SomeCoOrd_workflow_path=/path/to/workflow.xml
                                     |oozie.coord.application.path=somePath/coordinator.xml""".stripMargin)

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<coordinator-app
      name="SomeCoOrd"
      frequency="${SomeCoOrd_frequency}"
      start="${SomeCoOrd_start}"
      end="${SomeCoOrd_end}"
      timezone="${SomeCoOrd_timezone}"
      xmlns="uri:oozie:coordinator:0.4" xmlns:sla="uri:oozie:sla:0.2">
        <action>
          <workflow>
            <app-path>{"${SomeCoOrd_workflow_path}"}</app-path>
            <configuration>
              <property>
                <name>some</name>
                <value>{"${SomeCoOrd_property0}"}</value>
              </property>
              <property>
                <name>nominalTime</name>
                <value>{functionVal}</value>
              </property>
            </configuration>
          </workflow>
        </action>
      </coordinator-app>))
  }
}
