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

import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.exception._
import org.antipathy.scoozie.sla._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._
import scala.xml.Elem

class NodeSpec extends FlatSpec with Matchers {

  behavior of "Node"

  it should "raise an exception when ok to transition not been defined" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = EmailAction(name = "email",
                             to = Seq("a@a.com", "b@b.com"),
                             cc = Seq("c@c.com", "d@d.com"),
                             subject = "message subject",
                             body = "message body",
                             contentTypeOption = None)

    an[TransitionException] should be thrownBy {
      result.toXML
    }

  }

  it should "raise an exception when error to transition not been defined" in {
    implicit val credentialsOption: Option[Credentials] = None

    val sparkAction = SparkAction(name = "SomeAction",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq("one", "two", "three"),
                                  jobXmlOption = Some("/path/to/spark/settings"),
                                  prepareOption = None,
                                  configuration = Configuration(
                                    Seq(Property(name = "SomeProp1", "SomeValue1"),
                                        Property(name = "SomeProp2", "SomeValue2"))
                                  ),
                                  yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode"))

    val emailAction = EmailAction(name = "email",
                                  to = Seq("a@a.com", "b@b.com"),
                                  cc = Seq("c@c.com", "d@d.com"),
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)

    val result = sparkAction okTo emailAction

    an[TransitionException] should be thrownBy {
      result.toXML
    }

  }

  it should "generate correct xml for decisions" in {
    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    val predicateValue = "${somePredicate}"

    val result = Decision(name = "SomeDecision",
                          default = oozieNode,
                          Switch(oozieNode, "somePredicate"),
                          Switch(oozieNode, "somePredicate")).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<decision name="SomeDecision">
      <switch>
        <case to="SomeNode">{predicateValue}</case>
        <case to="SomeNode">{predicateValue}</case>
        <default to="SomeNode"/>
      </switch>
    </decision>))
  }

  it should "generate correct xml for Start" in {

    implicit val credentialsOption: Option[Credentials] = None
    val emailAction = EmailAction(name = "email",
                                  to = Seq("a@a.com", "b@b.com"),
                                  cc = Seq("c@c.com", "d@d.com"),
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)

    val result = Start() okTo emailAction

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<start to="email"/>))
  }

  it should "raise an exception if no transition from start has been defined" in {

    an[TransitionException] should be thrownBy {
      Start().toXML
    }
  }

  it should "generate correct xml for end" in {
    val result = new End().toXML
    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<end name="end" />))
  }
  it should "generate correct xml for kill" in {
    scala.xml.Utility.trim(Kill("Killed!").toXML) should be(scala.xml.Utility.trim(<kill name="kill">
        <message>Killed!</message>
      </kill>))
  }
  it should "generate correct xml for fork" in {
    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    val result = Fork("SomeFork", Seq(oozieNode, oozieNode)).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<fork name="SomeFork">
      <path start="SomeNode"/>
      <path start="SomeNode"/>
    </fork>))
  }
  it should "generate correct xml for join" in {
    val oozieNode = Node(new Action {
      override def name = "SomeNode"
      override def toXML: Elem = <action name={name}></action>
      override val xmlns: Option[String] = None
      override val properties: Map[String, String] = Map()
    })(None)

    val result = Join("SomeJoin", oozieNode).action.toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<join name="SomeJoin" to="SomeNode" />))
  }

  it should "generate correct xml for an action with no credentials" in {
    implicit val credentialsOption: Option[Credentials] = None

    val sparkAction = SparkAction(name = "SomeAction",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq("one", "two", "three"),
                                  jobXmlOption = Some("/path/to/spark/settings"),
                                  prepareOption = None,
                                  configuration = Configuration(
                                    Seq(Property(name = "SomeProp1", "SomeValue1"),
                                        Property(name = "SomeProp2", "SomeValue2"))
                                  ),
                                  yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode"))

    val emailAction = EmailAction(name = "email",
                                  to = Seq("a@a.com", "b@b.com"),
                                  cc = Seq("c@c.com", "d@d.com"),
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)

    val result = (sparkAction okTo emailAction errorTo emailAction).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<action name="SomeAction">
          <spark xmlns="uri:oozie:spark-action:0.1">
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <job-xml>{"${SomeAction_jobXml}"}</job-xml>
            <configuration>
              <property>
                <name>SomeProp1</name>
                <value>{"${SomeAction_property0}"}</value>
              </property>
              <property>
                <name>SomeProp2</name>
                <value>{"${SomeAction_property1}"}</value>
              </property>
            </configuration>
            <master>{"${SomeAction_sparkMasterURL}"}</master>
            <mode>{"${SomeAction_sparkMode}"}</mode>
            <name>{"${SomeAction_sparkJobName}"}</name>
            <class>{"${SomeAction_mainClass}"}</class>
            <jar>{"${SomeAction_sparkJar}"}</jar>
            <spark-opts>{"${SomeAction_sparkOptions}"}</spark-opts>
            <arg>{"${SomeAction_commandLineArg0}"}</arg>
            <arg>{"${SomeAction_commandLineArg1}"}</arg>
            <arg>{"${SomeAction_commandLineArg2}"}</arg>
          </spark>
          <ok to="email"/>
          <error to="email"/>
        </action>))
  }

  it should "generate correct xml for an action with credentials" in {
    implicit val credentialsOption: Option[Credentials] = Some(
      Credentials(
        Credential(name = "hive-credentials",
                   credentialsType = "hive",
                   properties = Seq(Property("hive2.jdbc.url", "jdbc:hive2://hiveserver2;ssl=true;")))
      )
    )

    val sparkAction = SparkAction(name = "SomeAction",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq("one", "two", "three"),
                                  jobXmlOption = Some("/path/to/spark/settings"),
                                  prepareOption = None,
                                  configuration = Configuration(
                                    Seq(Property(name = "SomeProp1", "SomeValue1"),
                                        Property(name = "SomeProp2", "SomeValue2"))
                                  ),
                                  yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode"))

    val emailAction = EmailAction(name = "email",
                                  to = Seq("a@a.com", "b@b.com"),
                                  cc = Seq("c@c.com", "d@d.com"),
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)

    val result = (sparkAction okTo emailAction errorTo emailAction).toXML

    scala.xml.Utility.trim(result) should be(scala.xml.Utility.trim(<action name="SomeAction" cred="hive-credentials">
          <spark xmlns="uri:oozie:spark-action:0.1">
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <job-xml>{"${SomeAction_jobXml}"}</job-xml>
            <configuration>
              <property>
                <name>SomeProp1</name>
                <value>{"${SomeAction_property0}"}</value>
              </property>
              <property>
                <name>SomeProp2</name>
                <value>{"${SomeAction_property1}"}</value>
              </property>
            </configuration>
            <master>{"${SomeAction_sparkMasterURL}"}</master>
            <mode>{"${SomeAction_sparkMode}"}</mode>
            <name>{"${SomeAction_sparkJobName}"}</name>
            <class>{"${SomeAction_mainClass}"}</class>
            <jar>{"${SomeAction_sparkJar}"}</jar>
            <spark-opts>{"${SomeAction_sparkOptions}"}</spark-opts>
            <arg>{"${SomeAction_commandLineArg0}"}</arg>
            <arg>{"${SomeAction_commandLineArg1}"}</arg>
            <arg>{"${SomeAction_commandLineArg2}"}</arg>
          </spark>
          <ok to="email"/>
          <error to="email"/>
        </action>))
  }

  it should "generate correct xml for an action with an sla" in {

    implicit val credentialsOption: Option[Credentials] = Some(
      Credentials(
        Credential(name = "hive-credentials",
                   credentialsType = "hive",
                   properties = Seq(Property("hive2.jdbc.url", "jdbc:hive2://hiveserver2;ssl=true;")))
      )
    )

    val sparkActionNoSLA = SparkAction(name = "SomeAction",
                                       sparkMasterURL = "masterURL",
                                       sparkMode = "mode",
                                       sparkJobName = "JobName",
                                       mainClass = "org.antipathy.Main",
                                       sparkJar = "/path/to/jar",
                                       sparkOptions = "spark options",
                                       commandLineArgs = Seq("one", "two", "three"),
                                       jobXmlOption = Some("/path/to/spark/settings"),
                                       prepareOption = None,
                                       configuration = Configuration(
                                         Seq(Property(name = "SomeProp1", "SomeValue1"),
                                             Property(name = "SomeProp2", "SomeValue2"))
                                       ),
                                       yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode"))

    val sla = OozieSLA(nominalTime = "nominal_time",
                       shouldStart = Some("10 * MINUTES"),
                       shouldEnd = Some("30 * MINUTES"),
                       maxDuration = Some("30 * MINUTES"))

    val sparkAction = sparkActionNoSLA.withSLA(sla)

    val emailAction = EmailAction(name = "email",
                                  to = Seq("a@a.com", "b@b.com"),
                                  cc = Seq("c@c.com", "d@d.com"),
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)

    val result = sparkAction okTo emailAction errorTo emailAction

    scala.xml.Utility.trim(result.toXML) should be(
      scala.xml.Utility.trim(<action name="SomeAction" cred="hive-credentials">
      <spark xmlns="uri:oozie:spark-action:0.1">
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <job-xml>{"${SomeAction_jobXml}"}</job-xml>
        <configuration>
          <property>
            <name>SomeProp1</name>
            <value>{"${SomeAction_property0}"}</value>
          </property>
          <property>
            <name>SomeProp2</name>
            <value>{"${SomeAction_property1}"}</value>
          </property>
        </configuration>
        <master>{"${SomeAction_sparkMasterURL}"}</master>
        <mode>{"${SomeAction_sparkMode}"}</mode>
        <name>{"${SomeAction_sparkJobName}"}</name>
        <class>{"${SomeAction_mainClass}"}</class>
        <jar>{"${SomeAction_sparkJar}"}</jar>
        <spark-opts>{"${SomeAction_sparkOptions}"}</spark-opts>
        <arg>{"${SomeAction_commandLineArg0}"}</arg>
        <arg>{"${SomeAction_commandLineArg1}"}</arg>
        <arg>{"${SomeAction_commandLineArg2}"}</arg>
      </spark>
      <ok to="email"/>
      <error to="email"/>
      <sla:info>
        <sla:nominal-time>{"${nominal_time}"}</sla:nominal-time>
        <sla:should-start>{"${SomeAction_sla_shouldStart}"}</sla:should-start>
        <sla:should-end>{"${SomeAction_sla_shouldStart}"}</sla:should-end>
        <sla:max-duration>{"${SomeAction_sla_maxDuration}"}</sla:max-duration>
      </sla:info>
    </action>)
    )

    result.properties should be(
      Map("${SomeAction_property1}" -> "SomeValue2",
          "${SomeAction_sparkMode}" -> "mode",
          "${SomeAction_sla_shouldStart}" -> "10 * MINUTES",
          "${SomeAction_commandLineArg0}" -> "one",
          "${SomeAction_jobXml}" -> "/path/to/spark/settings",
          "${SomeAction_sparkMasterURL}" -> "masterURL",
          "${SomeAction_commandLineArg2}" -> "three",
          "${SomeAction_sla_maxDuration}" -> "30 * MINUTES",
          "${SomeAction_sparkJobName}" -> "JobName",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_sparkOptions}" -> "spark options",
          "${SomeAction_sla_shouldEnd}" -> "30 * MINUTES",
          "${SomeAction_property0}" -> "SomeValue1",
          "${SomeAction_commandLineArg1}" -> "two",
          "${SomeAction_sparkJar}" -> "/path/to/jar")
    )
  }
}
