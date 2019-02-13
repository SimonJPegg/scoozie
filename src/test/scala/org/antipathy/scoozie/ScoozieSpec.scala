package org.antipathy.scoozie

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._
import scala.xml.Utility

class ScoozieSpec extends FlatSpec with Matchers {

  behavior of "Scoozie"

  it should "allow the creation of an oozie workflow" in {
    val testWorkflow = new TestJob("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))

    Utility.trim(testWorkflow.workflow.toXML) should be(
      Utility.trim(<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="ExampleWorkflow">
          <global>
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
          </global>
          <start to="sparkOrShell" />
          <decision name="sparkOrShell">
            <switch>
              <case to="doAShellThing">{"${someVar}"}</case>
              <default to="doASparkThing" />
            </switch>
          </decision>
          <action name="doAShellThing">
            <shell xmlns="uri:oozie:shell-action:0.1">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <exec>{"${doAShellThing_scriptName}"}</exec>
              <file>{"${doAShellThing_scriptLocation}#${doAShellThing_scriptName}"}</file>
              <capture-output />
            </shell>
            <ok to="mainFork" />
            <error to="alertFailure" />
          </action>
          <action name="doASparkThing">
            <spark xmlns="uri:oozie:spark-action:1.0">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${doASparkThing_jobXml}"}</job-xml>
              <master>{"${doASparkThing_sparkMasterURL}"}</master>
              <mode>{"${doASparkThing_sparkMode}"}</mode>
              <name>{"${doASparkThing_sparkJobName}"}</name>
              <class>{"${doASparkThing_mainClass}"}</class>
              <jar>{"${doASparkThing_sparkJar}"}</jar>
              <spark-opts>{"${doASparkThing_sparkOptions}"}</spark-opts>
            </spark>
            <ok to="mainFork" />
            <error to="alertFailure" />
            <sla:info>
              <sla:nominal-time>{"${doASparkThing_sla_nominalTime}"}</sla:nominal-time>
              <sla:should-start>{"${doASparkThing_sla_shouldStart}"}</sla:should-start>
              <sla:should-end>{"${doASparkThing_sla_shouldStart}"}</sla:should-end>
              <sla:max-duration>{"${doASparkThing_sla_maxDuration}"}</sla:max-duration>
              <sla:alert-events>{"${doASparkThing_sla_alertEvents}"}</sla:alert-events>
              <sla:alert-contact>{"${doASparkThing_sla_alertContacts}"}</sla:alert-contact>
            </sla:info>
          </action>
          <fork name="mainFork">
            <path start="doAJavaThing" />
            <path start="doAHiveThing" />
          </fork>
          <action name="doAJavaThing">
            <java>
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <prepare>
                <delete path="${doAJavaThing_prepare_delete}" />
              </prepare>
              <main-class>{"${doAJavaThing_mainClass}"}</main-class>
              <java-opts>{"${doAJavaThing_javaOptions}"}</java-opts>
              <file>{"${doAJavaThing_javaJar}"}</file>
            </java>
            <ok to="mainJoin" />
            <error to="alertFailure" />
          </action>
          <action name="doAHiveThing">
            <hive xmlns="uri:oozie:hive-action:0.2">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${doAHiveThing_jobXml}"}</job-xml>
              <script>{"${doAHiveThing_scriptName}"}</script>
              <file>{"${doAHiveThing_scriptLocation}"}</file>
            </hive>
            <ok to="mainJoin" />
            <error to="alertFailure" />
          </action>
          <join name="mainJoin" to="end" />
          <action name="alertFailure">
            <email xmlns="uri:oozie:email-action:0.1">
              <to>{"${alertFailure_to}"}</to>
              <subject>{"${alertFailure_subject}"}</subject>
              <body>{"${alertFailure_body}"}</body>
            </email>
            <ok to="kill" />
            <error to="kill" />
          </action>
          <kill name="kill">
            <message>Workflow failed</message>
          </kill>
          <end name="end" />
        </workflow-app>)
    )

    testWorkflow.jobProperties should be("""ExampleCoOrdinator_end=end
                                           |ExampleCoOrdinator_frequency=startFreq
                                           |ExampleCoOrdinator_property0=value1
                                           |ExampleCoOrdinator_property1=value2
                                           |ExampleCoOrdinator_start=start
                                           |ExampleCoOrdinator_timezone=timeZome
                                           |ExampleCoOrdinator_workflow_path=/path/to/workflow.xml
                                           |alertFailure_body=message body
                                           |alertFailure_subject=message subject
                                           |alertFailure_to=a@a.com,b@b.com
                                           |doAHiveThing_jobXml=/path/to/settings.xml
                                           |doAHiveThing_scriptLocation=/path/to/someScript.hql
                                           |doAHiveThing_scriptName=someScript.hql
                                           |doAJavaThing_javaJar=/path/to/jar
                                           |doAJavaThing_javaOptions=java options
                                           |doAJavaThing_mainClass=org.antipathy.Main
                                           |doAJavaThing_prepare_delete=/some/path
                                           |doAShellThing_scriptLocation=/path/to/script.sh
                                           |doAShellThing_scriptName=script.sh
                                           |doASparkThing_jobXml=/path/to/job/xml
                                           |doASparkThing_mainClass=org.antipathy.Main
                                           |doASparkThing_sla_alertContacts=some@one.com
                                           |doASparkThing_sla_alertEvents=start_miss,end_miss,duration_miss
                                           |doASparkThing_sla_maxDuration=30 * MINUTES
                                           |doASparkThing_sla_nominalTime=nominal_time
                                           |doASparkThing_sla_shouldEnd=30 * MINUTES
                                           |doASparkThing_sla_shouldStart=10 * MINUTES
                                           |doASparkThing_sparkJar=/path/to/jar
                                           |doASparkThing_sparkJobName=JobName
                                           |doASparkThing_sparkMasterURL=masterURL
                                           |doASparkThing_sparkMode=mode
                                           |doASparkThing_sparkOptions=spark options
                                           |jobTracker=yarn
                                           |nameNode=nameservice1""".stripMargin)
  }

  it should "allow validation of an oozie workflow" in {
    Scoozie.Test.validate(new TestJob("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2")).workflow)
  }

  it should "allow testing the transitions of an oozie workflow" in {

    Scoozie.Test
      .workflowTesterWorkflowTestRunner(
        new TestJob("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2")).workflow
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
    val testWorkflow = new TestJob("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))
    Utility.trim(testWorkflow.coordinator.toXML) should be(Utility.trim(<coordinator-app
        name="ExampleCoOrdinator"
        frequency="${ExampleCoOrdinator_frequency}"
        start="${ExampleCoOrdinator_start}" end="${ExampleCoOrdinator_end}"
        timezone="${ExampleCoOrdinator_timezone}"
        xmlns="uri:oozie:coordinator:0.4" xmlns:sla="uri:oozie:sla:0.2">
          <action>
            <workflow>
              <app-path>{"${ExampleCoOrdinator_workflow_path}"}</app-path>
              <configuration>
                <property><name>prop1</name>
                  <value>{"${ExampleCoOrdinator_property0}"}</value>
                </property>
                <property>
                  <name>prop2</name>
                  <value>{"${ExampleCoOrdinator_property1}"}</value>
                </property>
              </configuration>
            </workflow>
          </action>
        </coordinator-app>))
  }

  it should "allow validation of an oozie coordinator" in {
    Scoozie.Test.validate(
      new TestJob("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2")).coordinator
    )
  }

}
