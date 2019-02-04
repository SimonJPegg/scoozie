package org.antipathy.scoozie

import org.scalatest.{FlatSpec, Matchers}
import scala.collection.immutable._
import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.workflow.Workflow
import scala.xml

class ScoozieSpec extends FlatSpec with Matchers {

  behavior of "Scoozie"

  class TestWorkflow(jobTracker: String, nameNode: String, yarnProperties: Map[String, String]) {

    private implicit val credentials: Option[Credentials] = Scoozie.Config.emptyCredentials
    private val yarnConfig = Scoozie.Config.yarnConfiguration(jobTracker, nameNode, yarnProperties)
    private val kill = Scoozie.Action.kill("Workflow failed")

    private val sparkAction = Scoozie.Action.spark(name = "doASparkThing",
                                                   sparkSettings = "/path/to/spark/settings",
                                                   sparkMasterURL = "masterURL",
                                                   sparkMode = "mode",
                                                   sparkJobName = "JobName",
                                                   mainClass = "org.antipathy.Main",
                                                   sparkJar = "/path/to/jar",
                                                   sparkOptions = "spark options",
                                                   commandLineArgs = Seq(),
                                                   files = Seq(),
                                                   prepareOption = None,
                                                   config = yarnConfig)

    private val emailAction = Scoozie.Action.email(name = "alertFailure",
                                                   to = Seq("a@a.com", "b@b.com"),
                                                   subject = "message subject",
                                                   body = "message body")

    private val shellAction = Scoozie.Action.shell(name = "doAShellThing",
                                                   prepareOption = None,
                                                   scriptName = "script.sh",
                                                   scriptLocation = "/path/to/script.sh",
                                                   commandLineArgs = Seq(),
                                                   envVars = Seq(),
                                                   files = Seq(),
                                                   captureOutput = true,
                                                   config = yarnConfig)

    private val hiveAction = Scoozie.Action.hive(name = "doAHiveThing",
                                                 hiveSettingsXML = "/path/to/settings.xml",
                                                 scriptName = "someScript.hql",
                                                 scriptLocation = "/path/to/someScript.hql",
                                                 parameters = Seq(),
                                                 prepareOption = None,
                                                 config = yarnConfig)

    private val javaAction = Scoozie.Action.java(name = "doAJavaThing",
                                                 mainClass = "org.antipathy.Main",
                                                 javaJar = "/path/to/jar",
                                                 javaOptions = "java options",
                                                 commandLineArgs = Seq(),
                                                 captureOutput = false,
                                                 files = Seq(),
                                                 prepareOption = None,
                                                 config = yarnConfig)

    private val start = Scoozie.Action.start

    private val transitions = {
      val errorMail = emailAction okTo kill errorTo kill
      val mainJoin = Scoozie.Action.join("mainJoin", Scoozie.Action.end)
      val java = javaAction okTo mainJoin errorTo errorMail
      val hive = hiveAction okTo mainJoin errorTo errorMail
      val mainFork = Scoozie.Action.fork("mainFork", Seq(java, hive))
      val shell = shellAction okTo mainFork errorTo errorMail
      val spark = sparkAction okTo mainFork errorTo errorMail
      val decision = Scoozie.Action.decision("sparkOrShell", spark, Scoozie.Action.switch(shell, "${someVar}"))
      start okTo decision
    }

    val workflow: Workflow = Scoozie.workflow("ExampleWorkflow", "/path/to/workflow.xml", transitions, None, yarnConfig)

    val coOrdinator = Scoozie.coOrdinator("ExampleCoOrdinator", "startFreq", "start", "end", "timeZome", workflow)

    val jobConfig: String = workflow.jobProperties
  }

  it should "allow the creation of an oozie workflow" in {
    val testWorkflow = new TestWorkflow("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))

    val result = Scoozie.Format.format(testWorkflow.workflow, 80, 4) should be(
      """<workflow-app name="ExampleWorkflow" xmlns="uri:oozie:workflow:0.4">
        |    <global>
        |        <job-tracker>${jobTracker}</job-tracker>
        |        <name-node>${nameNode}</name-node>
        |    </global>
        |    <start to="sparkOrShell"/>
        |    <decision name="sparkOrShell">
        |        <switch>
        |            <case to="doAShellThing">${someVar}</case>
        |            <default to="doASparkThing"/>
        |        </switch>
        |    </decision>
        |    <action name="doAShellThing">
        |        <shell xmlns="uri:oozie:shell-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <configuration>
        |                <property>
        |                    <name>prop1</name>
        |                    <value>${doAShellThing_property0}</value>
        |                </property>
        |                <property>
        |                    <name>prop2</name>
        |                    <value>${doAShellThing_property1}</value>
        |                </property>
        |            </configuration>
        |            <exec>${doAShellThing_scriptName}</exec>
        |            <file>${doAShellThing_scriptLocation}#${doAShellThing_scriptName}</file>
        |            <capture-output/>
        |        </shell>
        |        <ok to="mainFork"/>
        |        <error to="alertFailure"/>
        |    </action>
        |    <action name="doASparkThing">
        |        <spark xmlns="uri:oozie:spark-action:1.0">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${doASparkThing_sparkSettings}</job-xml>
        |            <configuration>
        |                <property>
        |                    <name>prop1</name>
        |                    <value>${doASparkThing_property0}</value>
        |                </property>
        |                <property>
        |                    <name>prop2</name>
        |                    <value>${doASparkThing_property1}</value>
        |                </property>
        |            </configuration>
        |            <master>${doASparkThing_sparkMasterURL}</master>
        |            <mode>${doASparkThing_sparkMode}</mode>
        |            <name>${doASparkThing_sparkJobName}</name>
        |            <class>${doASparkThing_mainClass}</class>
        |            <jar>${doASparkThing_sparkJar}</jar>
        |            <spark-opts>${doASparkThing_sparkOptions}</spark-opts>
        |        </spark>
        |        <ok to="mainFork"/>
        |        <error to="alertFailure"/>
        |    </action>
        |    <fork name="mainFork">
        |        <path start="doAJavaThing"/>
        |        <path start="doAHiveThing"/>
        |    </fork>
        |    <action name="doAJavaThing">
        |        <java>
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <configuration>
        |                <property>
        |                    <name>prop1</name>
        |                    <value>${doAJavaThing_property0}</value>
        |                </property>
        |                <property>
        |                    <name>prop2</name>
        |                    <value>${doAJavaThing_property1}</value>
        |                </property>
        |            </configuration>
        |            <main-class>${doAJavaThing_mainClass}</main-class>
        |            <java-opts>${doAJavaThing_javaOptions}</java-opts>
        |            <file>${doAJavaThing_javaJar}</file>
        |        </java>
        |        <ok to="mainJoin"/>
        |        <error to="alertFailure"/>
        |    </action>
        |    <action name="doAHiveThing">
        |        <hive xmlns="uri:oozie:hive-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${doAHiveThing_hiveSettingsXML}</job-xml>
        |            <configuration>
        |                <property>
        |                    <name>prop1</name>
        |                    <value>${doAHiveThing_property0}</value>
        |                </property>
        |                <property>
        |                    <name>prop2</name>
        |                    <value>${doAHiveThing_property1}</value>
        |                </property>
        |            </configuration>
        |            <script>${doAHiveThing_scriptName}</script>
        |            <file>${doAHiveThing_scriptLocation}</file>
        |        </hive>
        |        <ok to="mainJoin"/>
        |        <error to="alertFailure"/>
        |    </action>
        |    <join name="mainJoin" to="end"/>
        |    <action name="alertFailure">
        |        <email xmlns="uri:oozie:email-action:0.1">
        |            <to>${alertFailure_to}</to>
        |            <subject>${alertFailure_subject}</subject>
        |            <body>${alertFailure_body}</body>
        |        </email>
        |        <ok to="kill"/>
        |        <error to="kill"/>
        |    </action>
        |    <kill name="kill">
        |        <message>Workflow failed</message>
        |    </kill>
        |    <end name="end"/>
        |</workflow-app>""".stripMargin
    )
  }

  it should "allow validation of an oozie workflow" in {
    Scoozie.Test.validate(
      new TestWorkflow("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2")).workflow
    )
  }

  it should "allow testing the transitions of an oozie workflow" in {

    Scoozie.Test
      .workflowTesterWorkflowTestRunner(
        new TestWorkflow("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2")).workflow
      )
      .traversalPath should be(
      "start -> " +
      "sparkOrShell -> " +
      "doASparkThing -> " +
      "mainFork -> " +
      "(doAJavaThing, doAHiveThing) -> " +
      "mainJoin -> " +
      "end"
    )
  }

  it should "allow the creation of an oozie coordinator" in {
    val testWorkflow = new TestWorkflow("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))
    xml.Utility.trim(testWorkflow.coOrdinator.toXML) should be(
      xml.Utility.trim(
        <coordinator-app
        name="ExampleCoOrdinator"
        frequency="startFreq"
        start="start" end="end"
        timezone="timeZome"
        xmlns="uri:oozie:coordinator:0.4">
          <action>
            <workflow>
              <app-path>/path/to/workflow.xml</app-path>
            </workflow>
          </action>
        </coordinator-app>
      )
    )
  }

  it should "allow validation of an oozie coordinator" in {
    Scoozie.Test.validate(
      new TestWorkflow("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2")).coOrdinator
    )
  }

}
