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
package org.antipathy.scoozie.workflow

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.action._
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.sla.OozieSLA
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

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
                                  cc = Seq.empty,
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)
      .okTo(kill)
      .errorTo(kill)

    val shellAction = ShellAction(name = "shellAction",
                                  scriptName = "script.sh",
                                  scriptLocation = "/path/to/script.sh",
                                  commandLineArgs = Seq(),
                                  envVars = Seq(),
                                  files = Seq(),
                                  captureOutput = true,
                                  jobXmlOption = None,
                                  prepareOption = None,
                                  configuration = Scoozie.Configuration.emptyConfig,
                                  yarnConfig = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val join = Join("mainJoin", shellAction)

    val sparkAction = SparkAction(name = "sparkAction",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq(),
                                  jobXmlOption = Some("/path/to/spark/settings"),
                                  prepareOption = None,
                                  configuration = Scoozie.Configuration.emptyConfig,
                                  yarnConfig = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                jobXmlOption = Some("/path/to/settings.xml"),
                                files = Seq(),
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                configuration = Scoozie.Configuration.emptyConfig,
                                yarnConfig = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val fork = Fork(name = "mainFork", Seq(sparkAction, hiveAction))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(fork),
                            jobXmlOption = None,
                            configuration = Configuration(
                              Seq(Property(name = "workflowprop1", value = "workflowpropvalue1"),
                                  Property(name = "workflowprop2", value = "workflowpropvalue2"))
                            ),
                            yarnConfig = yarnConfig)

    scala.xml.Utility.trim(workflow.toXML) should be(
      scala.xml.Utility
        .trim(<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="sampleWorkflow">
          <global>
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <configuration>
              <property>
                <name>workflowprop1</name>
                <value>{"${sampleWorkflow_property0}"}</value>
              </property>
              <property>
                <name>workflowprop2</name>
                <value>{"${sampleWorkflow_property1}"}</value>
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
              <job-xml>{"${sparkAction_jobXml}"}</job-xml>
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
              <job-xml>{"${hiveAction_jobXml}"}</job-xml>
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
          <action name="emailAction">
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

    workflow.jobProperties should be("""emailAction_body=message body
                                       |emailAction_subject=message subject
                                       |emailAction_to=a@a.com,b@b.com
                                       |hiveAction_jobXml=/path/to/settings.xml
                                       |hiveAction_scriptLocation=/path/to/someScript.hql
                                       |hiveAction_scriptName=someScript.hql
                                       |jobTracker=jobTracker
                                       |nameNode=nameNode
                                       |sampleWorkflow_credentialProperty0=value
                                       |sampleWorkflow_property0=workflowpropvalue1
                                       |sampleWorkflow_property1=workflowpropvalue2
                                       |shellAction_scriptLocation=/path/to/script.sh
                                       |shellAction_scriptName=script.sh
                                       |sparkAction_jobXml=/path/to/spark/settings
                                       |sparkAction_mainClass=org.antipathy.Main
                                       |sparkAction_sparkJar=/path/to/jar
                                       |sparkAction_sparkJobName=JobName
                                       |sparkAction_sparkMasterURL=masterURL
                                       |sparkAction_sparkMode=mode
                                       |sparkAction_sparkOptions=spark options
                                       |oozie.use.system.libpath=true
                                       |oozie.wf.application.path=/workflow.xml""".stripMargin)

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
                                  cc = Seq.empty,
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)
      .okTo(kill)
      .errorTo(kill)

    val shellAction = ShellAction(name = "shellAction",
                                  scriptName = "script.sh",
                                  scriptLocation = "/path/to/script.sh",
                                  commandLineArgs = Seq(),
                                  envVars = Seq(),
                                  files = Seq(),
                                  captureOutput = true,
                                  jobXmlOption = None,
                                  prepareOption = None,
                                  configuration = Scoozie.Configuration.emptyConfig,
                                  yarnConfig = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val sparkAction = SparkAction(name = "sparkAction",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq(),
                                  jobXmlOption = Some("/path/to/spark/settings"),
                                  prepareOption = None,
                                  configuration = Scoozie.Configuration.emptyConfig,
                                  yarnConfig = yarnConfig)
      .okTo(shellAction)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                jobXmlOption = Some("/path/to/settings.xml"),
                                files = Seq(),
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                configuration = Scoozie.Configuration.emptyConfig,
                                yarnConfig = yarnConfig)
      .okTo(shellAction)
      .errorTo(emailAction)

    val decision =
      Decision("doAThing", sparkAction, Switch(hiveAction, "somePredicate"), Switch(emailAction, "somePredicate"))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(decision),
                            jobXmlOption = Some("/path/to/job.xml"),
                            configuration = Scoozie.Configuration.emptyConfig,
                            yarnConfig = yarnConfig)

    scala.xml.Utility.trim(workflow.toXML) should be(
      scala.xml.Utility
        .trim(<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="sampleWorkflow">
          <global>
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <job-xml>{"${sampleWorkflow_jobXml}"}</job-xml>
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
              <job-xml>{"${hiveAction_jobXml}"}</job-xml>
              <script>{"${hiveAction_scriptName}"}</script>
              <file>{"${hiveAction_scriptLocation}"}</file>
            </hive>
            <ok to="shellAction" />
            <error to="emailAction" />
          </action>
          <action name="emailAction">
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
              <job-xml>{"${sparkAction_jobXml}"}</job-xml>
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

    workflow.jobProperties should be("""emailAction_body=message body
                                       |emailAction_subject=message subject
                                       |emailAction_to=a@a.com,b@b.com
                                       |hiveAction_jobXml=/path/to/settings.xml
                                       |hiveAction_scriptLocation=/path/to/someScript.hql
                                       |hiveAction_scriptName=someScript.hql
                                       |jobTracker=jobTracker
                                       |nameNode=nameNode
                                       |sampleWorkflow_credentialProperty0=value
                                       |sampleWorkflow_jobXml=/path/to/job.xml
                                       |shellAction_scriptLocation=/path/to/script.sh
                                       |shellAction_scriptName=script.sh
                                       |sparkAction_jobXml=/path/to/spark/settings
                                       |sparkAction_mainClass=org.antipathy.Main
                                       |sparkAction_sparkJar=/path/to/jar
                                       |sparkAction_sparkJobName=JobName
                                       |sparkAction_sparkMasterURL=masterURL
                                       |sparkAction_sparkMode=mode
                                       |sparkAction_sparkOptions=spark options
                                       |oozie.use.system.libpath=true
                                       |oozie.wf.application.path=/workflow.xml""".stripMargin)
  }

  it should "generate valid XML with an SLA" in {

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
                                  cc = Seq.empty,
                                  subject = "message subject",
                                  body = "message body",
                                  contentTypeOption = None)
      .okTo(kill)
      .errorTo(kill)

    val shellAction = ShellAction(name = "shellAction",
                                  scriptName = "script.sh",
                                  scriptLocation = "/path/to/script.sh",
                                  commandLineArgs = Seq(),
                                  envVars = Seq(),
                                  files = Seq(),
                                  captureOutput = true,
                                  jobXmlOption = None,
                                  prepareOption = None,
                                  configuration = Scoozie.Configuration.emptyConfig,
                                  yarnConfig = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val sparkAction = SparkAction(name = "sparkAction",
                                  sparkMasterURL = "masterURL",
                                  sparkMode = "mode",
                                  sparkJobName = "JobName",
                                  mainClass = "org.antipathy.Main",
                                  sparkJar = "/path/to/jar",
                                  sparkOptions = "spark options",
                                  commandLineArgs = Seq(),
                                  jobXmlOption = Some("/path/to/spark/settings"),
                                  prepareOption = None,
                                  configuration = Scoozie.Configuration.configuration(
                                    Seq(Property("someProperty", "somevalue"), Property("workflowId", "${wf:id()}"))
                                  ),
                                  yarnConfig = yarnConfig)
      .okTo(shellAction)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                jobXmlOption = Some("/path/to/settings.xml"),
                                files = Seq(),
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                configuration = Scoozie.Configuration.emptyConfig,
                                yarnConfig = yarnConfig)
      .okTo(shellAction)
      .errorTo(emailAction)

    val decision =
      Decision("doAThing", sparkAction, Switch(hiveAction, "somePredicate"), Switch(emailAction, "somePredicate"))

    val sla = OozieSLA(nominalTime = "${coord:nominal_time()}",
                       shouldStart = Some("10 * MINUTES"),
                       shouldEnd = Some("30 * MINUTES"),
                       maxDuration = Some("30 * MINUTES"))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(decision),
                            jobXmlOption = Some("/path/to/job.xml"),
                            configuration = Scoozie.Configuration.emptyConfig,
                            yarnConfig = yarnConfig,
                            slaOption = Some(sla))

    scala.xml.Utility.trim(workflow.toXML) should be(
      scala.xml.Utility
        .trim(<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="sampleWorkflow">
        <global>
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${sampleWorkflow_jobXml}"}</job-xml>
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
            <job-xml>{"${hiveAction_jobXml}"}</job-xml>
            <script>{"${hiveAction_scriptName}"}</script>
            <file>{"${hiveAction_scriptLocation}"}</file>
          </hive>
          <ok to="shellAction" />
          <error to="emailAction" />
        </action>
        <action name="emailAction">
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
            <job-xml>{"${sparkAction_jobXml}"}</job-xml>
            <configuration>
              <property>
                <name>someProperty</name>
                <value>{"${sparkAction_property0}"}</value>
              </property>
              <property>
                <name>workflowId</name>
                <value>{"${wf:id()}"}</value>
              </property>
            </configuration>
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
        <sla:info>
          <sla:nominal-time>{"${coord:nominal_time()}"}</sla:nominal-time>
          <sla:should-start>{"${sampleWorkflow_sla_shouldStart}"}</sla:should-start>
          <sla:should-end>{"${sampleWorkflow_sla_shouldEnd}"}</sla:should-end>
          <sla:max-duration>{"${sampleWorkflow_sla_maxDuration}"}</sla:max-duration>
        </sla:info>
      </workflow-app>)
    )

    workflow.jobProperties should be("""emailAction_body=message body
                                       |emailAction_subject=message subject
                                       |emailAction_to=a@a.com,b@b.com
                                       |hiveAction_jobXml=/path/to/settings.xml
                                       |hiveAction_scriptLocation=/path/to/someScript.hql
                                       |hiveAction_scriptName=someScript.hql
                                       |jobTracker=jobTracker
                                       |nameNode=nameNode
                                       |sampleWorkflow_credentialProperty0=value
                                       |sampleWorkflow_jobXml=/path/to/job.xml
                                       |sampleWorkflow_sla_maxDuration=30 * MINUTES
                                       |sampleWorkflow_sla_shouldEnd=30 * MINUTES
                                       |sampleWorkflow_sla_shouldStart=10 * MINUTES
                                       |shellAction_scriptLocation=/path/to/script.sh
                                       |shellAction_scriptName=script.sh
                                       |sparkAction_jobXml=/path/to/spark/settings
                                       |sparkAction_mainClass=org.antipathy.Main
                                       |sparkAction_property0=somevalue
                                       |sparkAction_sparkJar=/path/to/jar
                                       |sparkAction_sparkJobName=JobName
                                       |sparkAction_sparkMasterURL=masterURL
                                       |sparkAction_sparkMode=mode
                                       |sparkAction_sparkOptions=spark options
                                       |oozie.use.system.libpath=true
                                       |oozie.wf.application.path=/workflow.xml""".stripMargin)
  }

}
