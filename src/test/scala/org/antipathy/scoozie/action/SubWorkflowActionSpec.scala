package org.antipathy.scoozie.action

import org.antipathy.scoozie.configuration.{Configuration, Credentials, Property, YarnConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class SubWorkflowActionSpec extends FlatSpec with Matchers {

  behavior of "SubWorkflowAction"

  it should "generate valid XML with configuration propagated" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = SubWorkflowAction(name = "SomeAction",
                                   applicationPath = "/path/to/workflow.xml",
                                   propagateConfiguration = true,
                                   configuration = Configuration(Seq(Property("some", "value"))),
                                   yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<sub-workflow>
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
                                   configuration = Configuration(Seq(Property("some", "value"))),
                                   yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<sub-workflow>
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
