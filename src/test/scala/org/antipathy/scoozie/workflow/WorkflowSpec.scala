package org.antipathy.scoozie.workflow

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.action._
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.control._
import scala.xml
import scala.collection.immutable._
import org.antipathy.scoozie.Scoozie

class WorkflowSpec extends FlatSpec with Matchers {

  behavior of "Workflow"

  it should "generate valid XML when using forks" in {

    implicit val credentialsOption: Option[Credentials] = Some(
      Credentials(
        Credential(name = "hive-credentials",
                   credentialsType = "hive",
                   properties = Seq(Property(name = "name", value = "value")))
      )
    )

    val yarnConfig =
      YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")

    val kill = Kill("workflow failed")

    val emailAction = EmailAction(name = "emailAction",
                                  to = Seq("a@a.com", "b@b.com"),
                                  subject = "message subject",
                                  body = "message body")
      .okTo(kill)
      .errorTo(kill)

    val shellAction = ShellAction(name = "shellAction",
                                  prepareOption = None,
                                  scriptName = "script.sh",
                                  scriptLocation = "/path/to/script.sh",
                                  commandLineArgs = Seq(),
                                  envVars = Seq(),
                                  files = Seq(),
                                  captureOutput = true,
                                  configuration = Scoozie.Config.emptyConfiguration,
                                  yarnConfig = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val join = Join("mainJoin", shellAction)

    val sparkAction = SparkAction(name = "sparkAction",
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
                                  configuration = Scoozie.Config.emptyConfiguration,
                                  yarnConfig = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                configuration = Scoozie.Config.emptyConfiguration,
                                yarnConfig = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val fork = Fork(name = "mainFork", Seq(sparkAction, hiveAction))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(fork),
                            configurationOption =
                              Some(Configuration(Seq(Property(name = "workflowprop", value = "workflowpropvalue")))),
                            yarnConfig = yarnConfig)

    xml.Utility.trim(workflow.toXML) should be(
      xml.Utility.trim(<workflow-app xmlns="uri:oozie:workflow:0.4" name="sampleWorkflow">
          <global>
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <configuration>
              <property>
                <name>workflowprop</name>
                <value>{"${sampleWorkflow_property0}"}</value>
              </property>
            </configuration>
          </global>
          <credentials>
            <credential name="hive-credentials" type="hive">
              <property>
                <name>name</name>
                <value>{"${sampleWorkflow_credentialProperty0}"}</value>
              </property>
            </credential>
          </credentials>
          <start to="mainFork" />
          <fork name="mainFork">
            <path start="sparkAction" />
            <path start="hiveAction" />
          </fork>
          <action name="sparkAction" cred="hive-credentials">
            <spark xmlns="uri:oozie:spark-action:1.0">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${sparkAction_sparkSettings}"}</job-xml>
              <master>{"${sparkAction_sparkMasterURL}"}</master>
              <mode>{"${sparkAction_sparkMode}"}</mode>
              <name>{"${sparkAction_sparkJobName}"}</name>
              <class>{"${sparkAction_mainClass}"}</class>
              <jar>{"${sparkAction_sparkJar}"}</jar>
              <spark-opts>{"${sparkAction_sparkOptions}"}</spark-opts>
            </spark>
            <ok to="mainJoin" />
            <error to="emailAction" />
          </action>
          <action name="hiveAction" cred="hive-credentials">
            <hive xmlns="uri:oozie:hive-action:0.2">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${hiveAction_hiveSettingsXML}"}</job-xml>
              <script>{"${hiveAction_scriptName}"}</script>
              <file>{"${hiveAction_scriptLocation}"}</file>
            </hive>
            <ok to="mainJoin" />
            <error to="emailAction" />
          </action>
          <join name="mainJoin" to="shellAction" />
          <action name="shellAction" cred="hive-credentials">
            <shell xmlns="uri:oozie:shell-action:0.1">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <exec>{"${shellAction_scriptName}"}</exec>
              <file>{"${shellAction_scriptLocation}#${shellAction_scriptName}"}</file>
              <capture-output />
            </shell>
            <ok to="end" />
            <error to="emailAction" />
          </action>
          <action name="emailAction" cred="hive-credentials">
            <email xmlns="uri:oozie:email-action:0.1">
              <to>{"${emailAction_to}"}</to>
              <subject>{"${emailAction_subject}"}</subject>
              <body>{"${emailAction_body}"}</body>
            </email>
            <ok to="kill" />
            <error to="kill" />
          </action>
          <kill name="kill">
            <message>workflow failed</message>
          </kill>
          <end name="end" />
        </workflow-app>)
    )

    workflow.jobProperties should be("""sampleWorkflow_property0=workflowpropvalue
                                       |sparkAction_mainClass=org.antipathy.Main
                                       |hiveAction_hiveSettingsXML=/path/to/settings.xml
                                       |sampleWorkflow_credentialProperty0=value
                                       |sparkAction_sparkSettings=/path/to/spark/settings
                                       |emailAction_to=a@a.com,b@b.com
                                       |sparkAction_sparkMasterURL=masterURL
                                       |shellAction_scriptLocation=/path/to/script.sh
                                       |shellAction_scriptName=script.sh
                                       |sparkAction_sparkJobName=JobName
                                       |sparkAction_sparkOptions=spark options
                                       |nameNode=nameNode
                                       |sparkAction_sparkMode=mode
                                       |sparkAction_sparkJar=/path/to/jar
                                       |emailAction_body=message body
                                       |jobTracker=jobTracker
                                       |hiveAction_scriptName=someScript.hql
                                       |hiveAction_scriptLocation=/path/to/someScript.hql
                                       |emailAction_subject=message subject""".stripMargin)

  }

  it should "generate valid XML when using decisions" in {
    implicit val credentialsOption: Option[Credentials] = Some(
      Credentials(
        Credential(name = "hive-credentials",
                   credentialsType = "hive",
                   properties = Seq(Property(name = "name", value = "value")))
      )
    )

    val yarnConfig =
      YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")

    val kill = Kill("workflow failed")

    val emailAction = EmailAction(name = "emailAction",
                                  to = Seq("a@a.com", "b@b.com"),
                                  subject = "message subject",
                                  body = "message body")
      .okTo(kill)
      .errorTo(kill)

    val shellAction = ShellAction(name = "shellAction",
                                  prepareOption = None,
                                  scriptName = "script.sh",
                                  scriptLocation = "/path/to/script.sh",
                                  commandLineArgs = Seq(),
                                  envVars = Seq(),
                                  files = Seq(),
                                  captureOutput = true,
                                  configuration = Scoozie.Config.emptyConfiguration,
                                  yarnConfig = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val sparkAction = SparkAction(name = "sparkAction",
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
                                  configuration = Scoozie.Config.emptyConfiguration,
                                  yarnConfig = yarnConfig)
      .okTo(shellAction)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                configuration = Scoozie.Config.emptyConfiguration,
                                yarnConfig = yarnConfig)
      .okTo(shellAction)
      .errorTo(emailAction)

    val decision =
      Decision("doAThing", sparkAction, Switch(hiveAction, "somePredicate"), Switch(emailAction, "somePredicate"))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(decision),
                            configurationOption = None,
                            yarnConfig = yarnConfig)

    xml.Utility.trim(workflow.toXML) should be(
      xml.Utility.trim(<workflow-app xmlns="uri:oozie:workflow:0.4" name="sampleWorkflow">
          <global>
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
          </global>
          <credentials>
            <credential name="hive-credentials" type="hive">
              <property>
                <name>name</name>
                <value>{"${sampleWorkflow_credentialProperty0}"}</value>
              </property>
            </credential>
          </credentials>
          <start to="doAThing" />
          <decision name="doAThing">
            <switch>
              <case to="hiveAction">{"${somePredicate}"}</case>
              <case to="emailAction">{"${somePredicate}"}</case>
              <default to="sparkAction" />
            </switch>
          </decision>
          <action name="hiveAction" cred="hive-credentials">
            <hive xmlns="uri:oozie:hive-action:0.2">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${hiveAction_hiveSettingsXML}"}</job-xml>
              <script>{"${hiveAction_scriptName}"}</script>
              <file>{"${hiveAction_scriptLocation}"}</file>
            </hive>
            <ok to="shellAction" />
            <error to="emailAction" />
          </action>
          <action name="emailAction" cred="hive-credentials">
            <email xmlns="uri:oozie:email-action:0.1">
              <to>{"${emailAction_to}"}</to>
              <subject>{"${emailAction_subject}"}</subject>
              <body>{"${emailAction_body}"}</body>
            </email>
            <ok to="kill" />
            <error to="kill" />
          </action>
          <action name="sparkAction" cred="hive-credentials">
            <spark xmlns="uri:oozie:spark-action:1.0">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${sparkAction_sparkSettings}"}</job-xml>
              <master>{"${sparkAction_sparkMasterURL}"}</master>
              <mode>{"${sparkAction_sparkMode}"}</mode>
              <name>{"${sparkAction_sparkJobName}"}</name>
              <class>{"${sparkAction_mainClass}"}</class>
              <jar>{"${sparkAction_sparkJar}"}</jar>
              <spark-opts>{"${sparkAction_sparkOptions}"}</spark-opts>
            </spark>
            <ok to="shellAction" />
            <error to="emailAction" />
          </action>
          <action name="shellAction" cred="hive-credentials">
            <shell xmlns="uri:oozie:shell-action:0.1">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <exec>{"${shellAction_scriptName}"}</exec>
              <file>{"${shellAction_scriptLocation}#${shellAction_scriptName}"}</file>
              <capture-output />
            </shell>
            <ok to="end" />
            <error to="emailAction" />
          </action>
          <kill name="kill">
            <message>workflow failed</message>
          </kill>
          <end name="end" />
        </workflow-app>)
    )

    workflow.jobProperties should be("""sparkAction_mainClass=org.antipathy.Main
                                       |hiveAction_hiveSettingsXML=/path/to/settings.xml
                                       |sampleWorkflow_credentialProperty0=value
                                       |sparkAction_sparkSettings=/path/to/spark/settings
                                       |emailAction_to=a@a.com,b@b.com
                                       |sparkAction_sparkMasterURL=masterURL
                                       |shellAction_scriptLocation=/path/to/script.sh
                                       |shellAction_scriptName=script.sh
                                       |sparkAction_sparkJobName=JobName
                                       |sparkAction_sparkOptions=spark options
                                       |nameNode=nameNode
                                       |sparkAction_sparkMode=mode
                                       |sparkAction_sparkJar=/path/to/jar
                                       |emailAction_body=message body
                                       |jobTracker=jobTracker
                                       |hiveAction_scriptName=someScript.hql
                                       |hiveAction_scriptLocation=/path/to/someScript.hql
                                       |emailAction_subject=message subject""".stripMargin)
  }

}
