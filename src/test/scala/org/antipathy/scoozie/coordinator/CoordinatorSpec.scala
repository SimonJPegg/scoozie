package org.antipathy.scoozie.coordinator

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration.{Configuration, Property, Credentials, YarnConfig}
import scala.xml
import scala.collection.immutable._
import org.antipathy.scoozie.control.Start
import org.antipathy.scoozie.workflow.Workflow

class CoordinatorSpec extends FlatSpec with Matchers {


  behavior of "CoOrdinator"

  it should "generate valid XML without config" in {

    implicit val credentialsOption: Option[Credentials] = None

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "/path/to/workflow.xml",
                            transitions = Start(),
                            configurationOption = None,
                            yarnConfig = YarnConfig(jobTracker = "", nameNode = ""))

    val result = Coordinator(name = "SomeCoOrd",
                             frequency = "${coord:days(1)}",
                             start = "2009-01-02T08:00Z",
                             end = "2009-01-04T08:00Z",
                             timezone = "America/Los_Angeles",
                             workflow = workflow,
                             configurationOption = None).toXML

    xml.Utility.trim(result) should be(xml.Utility.trim(<coordinator-app
        name="SomeCoOrd"
        frequency="${coord:days(1)}"
        start="2009-01-02T08:00Z"
        end="2009-01-04T08:00Z"
        timezone="America/Los_Angeles"
        xmlns="uri:oozie:coordinator:0.1">
        <action>
          <workflow>
            <app-path>/path/to/workflow.xml</app-path>
          </workflow>
        </action>
      </coordinator-app>))
  }

  it should "generate valid XML with config" in {
    import org.antipathy.scoozie.configuration.{Credentials, YarnConfig}
    import org.antipathy.scoozie.control.Start
    import org.antipathy.scoozie.workflow.Workflow


    implicit val credentialsOption: Option[Credentials] = None

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "/path/to/workflow.xml",
                            transitions = Start(),
                            configurationOption = Some(Configuration(
                              Seq(Property("some", "value"))
                            )),
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "Namenode"))

    val result = Coordinator(name = "SomeCoOrd",
                             frequency = "${coord:days(1)}",
                             start = "2009-01-02T08:00Z",
                             end = "2009-01-04T08:00Z",
                             timezone = "America/Los_Angeles",
                             workflow = workflow,
                             configurationOption = Some(Configuration(
                               Seq(Property("some", "value"))
                             )))

    result.jobProperties should be (
      """SomeCoOrd_property0=value
        |sampleWorkflow_property0=value""".stripMargin)


    xml.Utility.trim(result.toXML) should be(xml.Utility.trim(<coordinator-app
      name="SomeCoOrd"
      frequency="${coord:days(1)}"
      start="2009-01-02T08:00Z"
      end="2009-01-04T08:00Z"
      timezone="America/Los_Angeles"
      xmlns="uri:oozie:coordinator:0.4">
        <action>
          <workflow>
            <app-path>/path/to/workflow.xml</app-path>
            <configuration>
              <property>
                <name>some</name>
                <value>{"${SomeCoOrd_property0}"}</value>
              </property>
            </configuration>
          </workflow>
        </action>
      </coordinator-app>))
  }
}