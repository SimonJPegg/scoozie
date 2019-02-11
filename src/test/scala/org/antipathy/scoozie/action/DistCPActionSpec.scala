package org.antipathy.scoozie.action

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.configuration.Credentials
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class DistCPActionSpec extends FlatSpec with Matchers {

  behavior of "DistCPAction"

  it should "generate valid XML" in {

    implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials

    val result = Scoozie.Actions
      .distCP("distCP",
              Scoozie.Configuration.emptyConfig,
              Scoozie.Configuration.yarnConfig("someJobTracker", "SomeNameNode"),
              Scoozie.Prepare.prepare(Seq(Scoozie.Prepare.delete("/some/path2"))),
              Seq("/some/path1", "/some/path2"),
              "-DskipTests=true")
      .action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<distcp xmlns="uri:oozie:distcp-action:0.2">
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <prepare>
          <delete path={"${distCP_prepare_delete}"}/>
        </prepare>
        <java-opts>{"${distCP_javaOptions}"}</java-opts>
        <arg>{"${distCP_arguments0}"}</arg>
        <arg>{"${distCP_arguments1}"}</arg>
      </distcp>))

    result.properties should be(
      Map("${distCP_javaOptions}" -> "-DskipTests=true",
          "${distCP_arguments0}" -> "/some/path1",
          "${distCP_arguments1}" -> "/some/path2",
          "${distCP_prepare_delete}" -> "/some/path2")
    )
  }

  it should "generate valid XML with config" in {

    implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials

    val additionalconfig = Map("somekey1" -> "somevalue1",
                               "somekey2" -> "somevalue2",
                               "somekey3" -> "somevalue3",
                               "somekey4" -> "somevalue4")

    val result = Scoozie.Actions
      .distCP("distCP",
              Scoozie.Configuration.configuration(additionalconfig),
              Scoozie.Configuration.yarnConfig("someJobTracker", "SomeNameNode"),
              Scoozie.Prepare.prepare(Seq(Scoozie.Prepare.delete("/some/path2"))),
              Seq("/some/path1", "/some/path2"),
              "-DskipTests=true")
      .action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<distcp xmlns="uri:oozie:distcp-action:0.2">
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <prepare>
          <delete path="${distCP_prepare_delete}"/>
        </prepare>
        <configuration>
          <property>
            <name>somekey1</name>
            <value>{"${distCP_property0}"}</value>
          </property>
          <property>
            <name>somekey2</name>
            <value>{"${distCP_property1}"}</value>
          </property>
          <property>
            <name>somekey3</name>
            <value>{"${distCP_property2}"}</value>
          </property>
          <property>
            <name>somekey4</name>
            <value>{"${distCP_property3}"}</value>
          </property>
        </configuration>
        <java-opts>{"${distCP_javaOptions}"}</java-opts>
        <arg>{"${distCP_arguments0}"}</arg>
        <arg>{"${distCP_arguments1}"}</arg>
      </distcp>))

    result.properties should be(
      Map("${distCP_property1}" -> "somevalue2",
          "${distCP_javaOptions}" -> "-DskipTests=true",
          "${distCP_property2}" -> "somevalue3",
          "${distCP_arguments1}" -> "/some/path2",
          "${distCP_arguments0}" -> "/some/path1",
          "${distCP_property3}" -> "somevalue4",
          "${distCP_property0}" -> "somevalue1",
          "${distCP_prepare_delete}" -> "/some/path2")
    )
  }
}
