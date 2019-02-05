package org.antipathy.scoozie.action

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration.{Configuration, Property, YarnConfig}
import org.antipathy.scoozie.configuration.Credentials
import scala.xml
import scala.collection.immutable._

class SubWorkflowActionSpec extends FlatSpec with Matchers {

  behavior of "SubWorkflowAction"

  it should "generate valid XML with configuration propagated" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = SubWorkflowAction(name = "SomeAction",
                                   applicationPath = "/path/to/workflow.xml",
                                   propagateConfiguration = true,
                                   config =
                                     YarnConfig(jobTracker = "jobTracker",
                                                nameNode = "nameNode",
                                                configuration = Configuration(Seq(Property("some", "value"))))).action

    xml.Utility.trim(result.toXML) should be(xml.Utility.trim(<sub-workflow>
        <app-path>{"${SomeAction_applicationPath}"}</app-path>
        <propagate-configuration/>
        <configuration>
          <property>
            <name>some</name>
            <value>{"${SomeAction_property0}"}</value>
          </property>
        </configuration>
      </sub-workflow>))

    result.properties should be(
      Map("${SomeAction_applicationPath}" -> "/path/to/workflow.xml", "${SomeAction_property0}" -> "value")
    )
  }

  it should "generate valid XML with its own configuration" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = SubWorkflowAction(name = "SomeAction",
                                   applicationPath = "/path/to/workflow.xml",
                                   propagateConfiguration = false,
                                   config =
                                     YarnConfig(jobTracker = "jobTracker",
                                                nameNode = "nameNode",
                                                configuration = Configuration(Seq(Property("some", "value"))))).action

    xml.Utility.trim(result.toXML) should be(xml.Utility.trim(<sub-workflow>
        <app-path>{"${SomeAction_applicationPath}"}</app-path>
        <configuration>
          <property>
            <name>some</name>
            <value>{"${SomeAction_property0}"}</value>
          </property>
        </configuration>
      </sub-workflow>))

    result.properties should be(
      Map("${SomeAction_applicationPath}" -> "/path/to/workflow.xml", "${SomeAction_property0}" -> "value")
    )
  }
}
