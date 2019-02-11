package org.antipathy.scoozie.action

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.action.control._
import scala.xml.Elem
import scala.collection.immutable._
import org.antipathy.scoozie.exception._

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
                                  sparkSettings = "/path/to/spark/settings",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq("one", "two", "three"),
                                  files = Seq(),
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
                                  sparkSettings = "/path/to/spark/settings",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq("one", "two", "three"),
                                  files = Seq(),
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
          <spark xmlns="uri:oozie:spark-action:1.0">
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <job-xml>{"${SomeAction_sparkSettings}"}</job-xml>
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
                                  sparkSettings = "/path/to/spark/settings",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq("one", "two", "three"),
                                  files = Seq(),
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
          <spark xmlns="uri:oozie:spark-action:1.0">
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <job-xml>{"${SomeAction_sparkSettings}"}</job-xml>
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
}
