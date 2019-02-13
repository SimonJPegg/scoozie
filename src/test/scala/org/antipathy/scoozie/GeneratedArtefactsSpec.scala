package org.antipathy.scoozie

import better.files._
import org.antipathy.scoozie.exception.TransitionException
import org.antipathy.scoozie.io.ArtefactWriter
import org.scalatest.{FlatSpec, Matchers}

import scala.util._

class GeneratedArtefactsSpec extends FlatSpec with Matchers {

  behavior of "GeneratedArtefacts"

  it should "generate a workflow" in {

    val configPath = File("src/test/resources/conf/workflowOnly.conf").path
    val outputPath = File("src/test/resources/output/generatedArtefacts/workflowOnly")
    val artefacts = Scoozie.fromConfig(configPath)
    artefacts.saveToPath(outputPath.path)

    val ouputWorkflow = outputPath.path.toString / ArtefactWriter.workflowFileName
    val ouputProperties = outputPath.path.toString / ArtefactWriter.propertiesFileName

    ouputWorkflow.lines.mkString(System.lineSeparator()) should be(
      """<workflow-app name="someworkflow" xmlns:sla="uri:oozie:sla:0.2" xmlns="uri:oozie:workflow:0.5">
        |    <global>
        |        <job-tracker>${jobTracker}</job-tracker>
        |        <name-node>${nameNode}</name-node>
        |        <job-xml>${someworkflow_jobXml}</job-xml>
        |        <configuration>
        |            <property>
        |                <name>workflow1</name>
        |                <value>${someworkflow_property0}</value>
        |            </property>
        |            <property>
        |                <name>workflow2</name>
        |                <value>${someworkflow_property1}</value>
        |            </property>
        |            <property>
        |                <name>workflow3</name>
        |                <value>${someworkflow_property2}</value>
        |            </property>
        |            <property>
        |                <name>workflow4</name>
        |                <value>${someworkflow_property3}</value>
        |            </property>
        |        </configuration>
        |    </global>
        |    <credentials>
        |        <credential name="someCredentials" type="credentialsType">
        |            <property>
        |                <name>credentials1</name>
        |                <value>${someworkflow_credentialProperty0}</value>
        |            </property>
        |            <property>
        |                <name>credentials2</name>
        |                <value>${someworkflow_credentialProperty1}</value>
        |            </property>
        |            <property>
        |                <name>credentials3</name>
        |                <value>${someworkflow_credentialProperty2}</value>
        |            </property>
        |            <property>
        |                <name>credentials4</name>
        |                <value>${someworkflow_credentialProperty3}</value>
        |            </property>
        |        </credential>
        |    </credentials>
        |    <start to="decisionNode"/>
        |    <decision name="decisionNode">
        |        <switch>
        |            <case to="hiveAction">${someOtherVar eq someVar}</case>
        |            <case to="sparkAction">${someVar eq 1}</case>
        |            <default to="sparkAction"/>
        |        </switch>
        |    </decision>
        |    <action name="hiveAction" cred="someCredentials">
        |        <hive xmlns="uri:oozie:hive-action:0.5">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${hiveAction_jobXml}</job-xml>
        |            <script>${hiveAction_scriptName}</script>
        |            <file>${hiveAction_scriptLocation}</file>
        |        </hive>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="sparkAction" cred="someCredentials">
        |        <spark xmlns="uri:oozie:spark-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <prepare>
        |                <delete path="${sparkAction_prepare_delete}"/>
        |                <mkdir path="${sparkAction_prepare_makedir}"/>
        |            </prepare>
        |            <job-xml>${sparkAction_jobXml}</job-xml>
        |            <master>${sparkAction_sparkMasterURL}</master>
        |            <mode>${sparkAction_sparkMode}</mode>
        |            <name>${sparkAction_sparkJobName}</name>
        |            <class>${sparkAction_mainClass}</class>
        |            <jar>${sparkAction_sparkJar}</jar>
        |            <spark-opts>${sparkAction_sparkOptions}</spark-opts>
        |        </spark>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="shellAction" cred="someCredentials">
        |        <shell xmlns="uri:oozie:shell-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <exec>${shellAction_scriptName}</exec>
        |            <file>${shellAction_scriptLocation}#${shellAction_scriptName}</file>
        |        </shell>
        |        <ok to="end"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="errorEmail">
        |        <email xmlns="uri:oozie:email-action:0.2">
        |            <to>${errorEmail_to}</to>
        |            <subject>${errorEmail_subject}</subject>
        |            <body>${errorEmail_body}</body>
        |        </email>
        |        <ok to="kill"/>
        |        <error to="kill"/>
        |    </action>
        |    <kill name="kill">
        |        <message>workflow is kill</message>
        |    </kill>
        |    <end name="end"/>
        |</workflow-app>""".stripMargin
    )
    ouputProperties.lines.mkString(System.lineSeparator()) should be(
      """errorEmail_body=yep
        |errorEmail_subject=hello
        |errorEmail_to=a@a.com
        |hiveAction_jobXml=settings
        |hiveAction_scriptLocation=/some/location
        |hiveAction_scriptName=script.hql
        |jobTracker=someNameNode
        |nameNode=someJobTracker
        |shellAction_scriptLocation=/some/location
        |shellAction_scriptName=script.sh
        |someworkflow_credentialProperty0="value1"
        |someworkflow_credentialProperty1="value2"
        |someworkflow_credentialProperty2="value3"
        |someworkflow_credentialProperty3="value4"
        |someworkflow_jobXml=/path/to/job.xml
        |someworkflow_property0="value1"
        |someworkflow_property1="value2"
        |someworkflow_property2="value3"
        |someworkflow_property3="value4"
        |sparkAction_jobXml=someSettings
        |sparkAction_mainClass=somemainclass
        |sparkAction_prepare_delete="deletePath"
        |sparkAction_prepare_makedir="makePath"
        |sparkAction_sparkJar=spark.jar
        |sparkAction_sparkJobName=Jobname
        |sparkAction_sparkMasterURL=masterurl
        |sparkAction_sparkMode=mode
        |sparkAction_sparkOptions=spark-options""".stripMargin
    )

    outputPath.delete()
  }

  it should "generate a workflow and a coordinator" in {

    val configPath = File("src/test/resources/conf/workflowAndCoordinator.conf").path
    val outputPath = File("src/test/resources/output/generatedArtefacts/workflowAndCoordinator")
    val artefacts = Scoozie.fromConfig(configPath)
    artefacts.saveToPath(outputPath.path)

    val outputCoordinator = outputPath.path.toString / ArtefactWriter.coordinatorFileName
    val ouputWorkflow = outputPath.path.toString / ArtefactWriter.workflowFileName
    val ouputProperties = outputPath.path.toString / ArtefactWriter.propertiesFileName

    outputCoordinator.lines.mkString(System.lineSeparator()) should be(
      "<coordinator-app \n" + //ffs
      """name="someCoordinator" frequency="${someCoordinator_frequency}" start="${someCoordinator_start}" end="${someCoordinator_end}" timezone="${someCoordinator_timezone}" xmlns:sla="uri:oozie:sla:0.2" xmlns="uri:oozie:coordinator:0.4">
        |    <action>
        |        <workflow>
        |            <app-path>${someCoordinator_workflow_path}</app-path>
        |            <configuration>
        |                <property>
        |                    <name>prop1</name>
        |                    <value>${someCoordinator_property0}</value>
        |                </property>
        |                <property>
        |                    <name>prop2</name>
        |                    <value>${someCoordinator_property1}</value>
        |                </property>
        |                <property>
        |                    <name>prop3</name>
        |                    <value>${someCoordinator_property2}</value>
        |                </property>
        |                <property>
        |                    <name>prop4</name>
        |                    <value>${someCoordinator_property3}</value>
        |                </property>
        |            </configuration>
        |        </workflow>
        |    </action>
        |</coordinator-app>""".stripMargin
    )

    ouputWorkflow.lines.mkString(System.lineSeparator()) should be(
      """<workflow-app name="someworkflow" xmlns:sla="uri:oozie:sla:0.2" xmlns="uri:oozie:workflow:0.5">
        |    <global>
        |        <job-tracker>${jobTracker}</job-tracker>
        |        <name-node>${nameNode}</name-node>
        |        <configuration>
        |            <property>
        |                <name>workflow1</name>
        |                <value>${someworkflow_property0}</value>
        |            </property>
        |            <property>
        |                <name>workflow2</name>
        |                <value>${someworkflow_property1}</value>
        |            </property>
        |            <property>
        |                <name>workflow3</name>
        |                <value>${someworkflow_property2}</value>
        |            </property>
        |            <property>
        |                <name>workflow4</name>
        |                <value>${someworkflow_property3}</value>
        |            </property>
        |        </configuration>
        |    </global>
        |    <credentials>
        |        <credential name="someCredentials" type="credentialsType">
        |            <property>
        |                <name>credentials1</name>
        |                <value>${someworkflow_credentialProperty0}</value>
        |            </property>
        |            <property>
        |                <name>credentials2</name>
        |                <value>${someworkflow_credentialProperty1}</value>
        |            </property>
        |            <property>
        |                <name>credentials3</name>
        |                <value>${someworkflow_credentialProperty2}</value>
        |            </property>
        |            <property>
        |                <name>credentials4</name>
        |                <value>${someworkflow_credentialProperty3}</value>
        |            </property>
        |        </credential>
        |    </credentials>
        |    <start to="decisionNode"/>
        |    <decision name="decisionNode">
        |        <switch>
        |            <case to="hiveAction">${someOtherVar eq someVar}</case>
        |            <case to="sparkAction">${someVar eq 1}</case>
        |            <default to="sparkAction"/>
        |        </switch>
        |    </decision>
        |    <action name="hiveAction" cred="someCredentials">
        |        <hive xmlns="uri:oozie:hive-action:0.5">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${hiveAction_jobXml}</job-xml>
        |            <script>${hiveAction_scriptName}</script>
        |            <file>${hiveAction_scriptLocation}</file>
        |        </hive>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="sparkAction" cred="someCredentials">
        |        <spark xmlns="uri:oozie:spark-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <prepare>
        |                <delete path="${sparkAction_prepare_delete}"/>
        |                <mkdir path="${sparkAction_prepare_makedir}"/>
        |            </prepare>
        |            <job-xml>${sparkAction_jobXml}</job-xml>
        |            <master>${sparkAction_sparkMasterURL}</master>
        |            <mode>${sparkAction_sparkMode}</mode>
        |            <name>${sparkAction_sparkJobName}</name>
        |            <class>${sparkAction_mainClass}</class>
        |            <jar>${sparkAction_sparkJar}</jar>
        |            <spark-opts>${sparkAction_sparkOptions}</spark-opts>
        |        </spark>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="shellAction" cred="someCredentials">
        |        <shell xmlns="uri:oozie:shell-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <exec>${shellAction_scriptName}</exec>
        |            <file>${shellAction_scriptLocation}#${shellAction_scriptName}</file>
        |        </shell>
        |        <ok to="end"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="errorEmail">
        |        <email xmlns="uri:oozie:email-action:0.2">
        |            <to>${errorEmail_to}</to>
        |            <subject>${errorEmail_subject}</subject>
        |            <body>${errorEmail_body}</body>
        |        </email>
        |        <ok to="kill"/>
        |        <error to="kill"/>
        |    </action>
        |    <kill name="kill">
        |        <message>workflow is kill</message>
        |    </kill>
        |    <end name="end"/>
        |</workflow-app>""".stripMargin
    )
    ouputProperties.lines.mkString(System.lineSeparator()) should be(
      """someCoordinator_end=someEnd
        |someCoordinator_frequency=someFreq
        |someCoordinator_property0="value1"
        |someCoordinator_property1="value2"
        |someCoordinator_property2="value3"
        |someCoordinator_property3="value4"
        |someCoordinator_start=someStart
        |someCoordinator_timezone=someTimezone
        |someCoordinator_workflow_path=somepath
        |errorEmail_body=yep
        |errorEmail_subject=hello
        |errorEmail_to=a@a.com
        |hiveAction_jobXml=settings
        |hiveAction_scriptLocation=/some/location
        |hiveAction_scriptName=script.hql
        |jobTracker=someNameNode
        |nameNode=someJobTracker
        |shellAction_scriptLocation=/some/location
        |shellAction_scriptName=script.sh
        |someworkflow_credentialProperty0="value1"
        |someworkflow_credentialProperty1="value2"
        |someworkflow_credentialProperty2="value3"
        |someworkflow_credentialProperty3="value4"
        |someworkflow_property0="value1"
        |someworkflow_property1="value2"
        |someworkflow_property2="value3"
        |someworkflow_property3="value4"
        |sparkAction_jobXml=someSettings
        |sparkAction_mainClass=somemainclass
        |sparkAction_prepare_delete="deletePath"
        |sparkAction_prepare_makedir="makePath"
        |sparkAction_sparkJar=spark.jar
        |sparkAction_sparkJobName=Jobname
        |sparkAction_sparkMasterURL=masterurl
        |sparkAction_sparkMode=mode
        |sparkAction_sparkOptions=spark-options""".stripMargin
    )

    outputPath.delete()
  }

  it should "generate a workflow, coordinator and a validation string" in {

    val configPath = File("src/test/resources/conf/workflowCoordinatorAndValidation.conf").path
    val outputPath = File("src/test/resources/output/generatedArtefacts/workflowCoordinatorAndValidation")
    val artefacts = Scoozie.fromConfig(configPath)
    artefacts.saveToPath(outputPath.path)

    val outputCoordinator = outputPath.path.toString / ArtefactWriter.coordinatorFileName
    val ouputWorkflow = outputPath.path.toString / ArtefactWriter.workflowFileName
    val ouputProperties = outputPath.path.toString / ArtefactWriter.propertiesFileName

    outputCoordinator.lines.mkString(System.lineSeparator()) should be(
      "<coordinator-app \n" + //ffs
      """name="someCoordinator" frequency="${someCoordinator_frequency}" start="${someCoordinator_start}" end="${someCoordinator_end}" timezone="${someCoordinator_timezone}" xmlns:sla="uri:oozie:sla:0.2" xmlns="uri:oozie:coordinator:0.4">
        |    <action>
        |        <workflow>
        |            <app-path>${someCoordinator_workflow_path}</app-path>
        |            <configuration>
        |                <property>
        |                    <name>prop1</name>
        |                    <value>${someCoordinator_property0}</value>
        |                </property>
        |                <property>
        |                    <name>prop2</name>
        |                    <value>${someCoordinator_property1}</value>
        |                </property>
        |                <property>
        |                    <name>prop3</name>
        |                    <value>${someCoordinator_property2}</value>
        |                </property>
        |                <property>
        |                    <name>prop4</name>
        |                    <value>${someCoordinator_property3}</value>
        |                </property>
        |            </configuration>
        |        </workflow>
        |    </action>
        |</coordinator-app>""".stripMargin
    )

    ouputWorkflow.lines.mkString(System.lineSeparator()) should be(
      """<workflow-app name="someworkflow" xmlns:sla="uri:oozie:sla:0.2" xmlns="uri:oozie:workflow:0.5">
        |    <global>
        |        <job-tracker>${jobTracker}</job-tracker>
        |        <name-node>${nameNode}</name-node>
        |        <configuration>
        |            <property>
        |                <name>workflow1</name>
        |                <value>${someworkflow_property0}</value>
        |            </property>
        |            <property>
        |                <name>workflow2</name>
        |                <value>${someworkflow_property1}</value>
        |            </property>
        |            <property>
        |                <name>workflow3</name>
        |                <value>${someworkflow_property2}</value>
        |            </property>
        |            <property>
        |                <name>workflow4</name>
        |                <value>${someworkflow_property3}</value>
        |            </property>
        |        </configuration>
        |    </global>
        |    <credentials>
        |        <credential name="someCredentials" type="credentialsType">
        |            <property>
        |                <name>credentials1</name>
        |                <value>${someworkflow_credentialProperty0}</value>
        |            </property>
        |            <property>
        |                <name>credentials2</name>
        |                <value>${someworkflow_credentialProperty1}</value>
        |            </property>
        |            <property>
        |                <name>credentials3</name>
        |                <value>${someworkflow_credentialProperty2}</value>
        |            </property>
        |            <property>
        |                <name>credentials4</name>
        |                <value>${someworkflow_credentialProperty3}</value>
        |            </property>
        |        </credential>
        |    </credentials>
        |    <start to="decisionNode"/>
        |    <decision name="decisionNode">
        |        <switch>
        |            <case to="hiveAction">${someOtherVar eq someVar}</case>
        |            <case to="sparkAction">${someVar eq 1}</case>
        |            <default to="sparkAction"/>
        |        </switch>
        |    </decision>
        |    <action name="hiveAction" cred="someCredentials">
        |        <hive xmlns="uri:oozie:hive-action:0.5">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${hiveAction_jobXml}</job-xml>
        |            <script>${hiveAction_scriptName}</script>
        |            <file>${hiveAction_scriptLocation}</file>
        |        </hive>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="sparkAction" cred="someCredentials">
        |        <spark xmlns="uri:oozie:spark-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <prepare>
        |                <delete path="${sparkAction_prepare_delete}"/>
        |                <mkdir path="${sparkAction_prepare_makedir}"/>
        |            </prepare>
        |            <job-xml>${sparkAction_jobXml}</job-xml>
        |            <master>${sparkAction_sparkMasterURL}</master>
        |            <mode>${sparkAction_sparkMode}</mode>
        |            <name>${sparkAction_sparkJobName}</name>
        |            <class>${sparkAction_mainClass}</class>
        |            <jar>${sparkAction_sparkJar}</jar>
        |            <spark-opts>${sparkAction_sparkOptions}</spark-opts>
        |        </spark>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="shellAction" cred="someCredentials">
        |        <shell xmlns="uri:oozie:shell-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <exec>${shellAction_scriptName}</exec>
        |            <file>${shellAction_scriptLocation}#${shellAction_scriptName}</file>
        |        </shell>
        |        <ok to="end"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="errorEmail">
        |        <email xmlns="uri:oozie:email-action:0.2">
        |            <to>${errorEmail_to}</to>
        |            <subject>${errorEmail_subject}</subject>
        |            <body>${errorEmail_body}</body>
        |        </email>
        |        <ok to="kill"/>
        |        <error to="kill"/>
        |    </action>
        |    <kill name="kill">
        |        <message>workflow is kill</message>
        |    </kill>
        |    <end name="end"/>
        |</workflow-app>""".stripMargin
    )
    ouputProperties.lines.mkString(System.lineSeparator()) should be(
      """someCoordinator_end=someEnd
        |someCoordinator_frequency=someFreq
        |someCoordinator_property0="value1"
        |someCoordinator_property1="value2"
        |someCoordinator_property2="value3"
        |someCoordinator_property3="value4"
        |someCoordinator_start=someStart
        |someCoordinator_timezone=someTimezone
        |someCoordinator_workflow_path=somepath
        |errorEmail_body=yep
        |errorEmail_subject=hello
        |errorEmail_to=a@a.com
        |hiveAction_jobXml=settings
        |hiveAction_scriptLocation=/some/location
        |hiveAction_scriptName=script.hql
        |jobTracker=someNameNode
        |nameNode=someJobTracker
        |shellAction_scriptLocation=/some/location
        |shellAction_scriptName=script.sh
        |someworkflow_credentialProperty0="value1"
        |someworkflow_credentialProperty1="value2"
        |someworkflow_credentialProperty2="value3"
        |someworkflow_credentialProperty3="value4"
        |someworkflow_property0="value1"
        |someworkflow_property1="value2"
        |someworkflow_property2="value3"
        |someworkflow_property3="value4"
        |sparkAction_jobXml=someSettings
        |sparkAction_mainClass=somemainclass
        |sparkAction_prepare_delete="deletePath"
        |sparkAction_prepare_makedir="makePath"
        |sparkAction_sparkJar=spark.jar
        |sparkAction_sparkJobName=Jobname
        |sparkAction_sparkMasterURL=masterurl
        |sparkAction_sparkMode=mode
        |sparkAction_sparkOptions=spark-options""".stripMargin
    )

    outputPath.delete()
  }

  it should "raise an error when the transistions do not match the validation string" in {

    val configPath = File("src/test/resources/conf/invalidValidationString.conf").path
    val outputPath = File("src/test/resources/output/generatedArtefacts/invalidValidationString")
    val artefacts = Scoozie.fromConfig(configPath)

    an[TransitionException] should be thrownBy {
      Try {
        artefacts.saveToPath(outputPath.path)
      } match {
        case Success(v) => v
        case Failure(e) =>
          e.getMessage should be("""Expected transition:
                                   |start -> sparkAction -> end
                                   |Actual transition:
                                   |start -> decisionNode -> sparkAction -> shellAction -> end""".stripMargin)
          throw e
      }
    }
  }

  it should "build a workflow with SLAs defined" in {
    val configPath = File("src/test/resources/conf/slas.conf").path
    val outputPath = File("src/test/resources/output/generatedArtefacts/slas")
    val artefacts = Scoozie.fromConfig(configPath)
    artefacts.saveToPath(outputPath.path)

    val outputCoordinator = outputPath.path.toString / ArtefactWriter.coordinatorFileName
    val ouputWorkflow = outputPath.path.toString / ArtefactWriter.workflowFileName
    val ouputProperties = outputPath.path.toString / ArtefactWriter.propertiesFileName

    outputCoordinator.lines.mkString(System.lineSeparator()) should be(
      """<coordinator-app 
        |name="someCoordinator" frequency="${someCoordinator_frequency}" start="${someCoordinator_start}" end="${someCoordinator_end}" timezone="${someCoordinator_timezone}" xmlns:sla="uri:oozie:sla:0.2" xmlns="uri:oozie:coordinator:0.4">
        |    <action>
        |        <workflow>
        |            <app-path>${someCoordinator_workflow_path}</app-path>
        |            <configuration>
        |                <property>
        |                    <name>prop1</name>
        |                    <value>${someCoordinator_property0}</value>
        |                </property>
        |                <property>
        |                    <name>prop2</name>
        |                    <value>${someCoordinator_property1}</value>
        |                </property>
        |                <property>
        |                    <name>prop3</name>
        |                    <value>${someCoordinator_property2}</value>
        |                </property>
        |                <property>
        |                    <name>prop4</name>
        |                    <value>${someCoordinator_property3}</value>
        |                </property>
        |            </configuration>
        |        </workflow>
        |        <sla:info>
        |            <sla:nominal-time>${someCoordinator_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${someCoordinator_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${someCoordinator_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${someCoordinator_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${someCoordinator_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${someCoordinator_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${someCoordinator_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${someCoordinator_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |</coordinator-app>""".stripMargin
    )

    ouputWorkflow.lines.mkString(System.lineSeparator()) should be(
      """<workflow-app name="someworkflow" xmlns:sla="uri:oozie:sla:0.2" xmlns="uri:oozie:workflow:0.5">
        |    <global>
        |        <job-tracker>${jobTracker}</job-tracker>
        |        <name-node>${nameNode}</name-node>
        |        <configuration>
        |            <property>
        |                <name>workflow1</name>
        |                <value>${someworkflow_property0}</value>
        |            </property>
        |            <property>
        |                <name>workflow2</name>
        |                <value>${someworkflow_property1}</value>
        |            </property>
        |            <property>
        |                <name>workflow3</name>
        |                <value>${someworkflow_property2}</value>
        |            </property>
        |            <property>
        |                <name>workflow4</name>
        |                <value>${someworkflow_property3}</value>
        |            </property>
        |        </configuration>
        |    </global>
        |    <credentials>
        |        <credential name="someCredentials" type="credentialsType">
        |            <property>
        |                <name>credentials1</name>
        |                <value>${someworkflow_credentialProperty0}</value>
        |            </property>
        |            <property>
        |                <name>credentials2</name>
        |                <value>${someworkflow_credentialProperty1}</value>
        |            </property>
        |            <property>
        |                <name>credentials3</name>
        |                <value>${someworkflow_credentialProperty2}</value>
        |            </property>
        |            <property>
        |                <name>credentials4</name>
        |                <value>${someworkflow_credentialProperty3}</value>
        |            </property>
        |        </credential>
        |    </credentials>
        |    <start to="decisionNode"/>
        |    <decision name="decisionNode">
        |        <switch>
        |            <case to="hiveAction">${someOtherVar eq someVar}</case>
        |            <case to="sparkAction">${someVar eq 1}</case>
        |            <default to="sparkAction"/>
        |        </switch>
        |    </decision>
        |    <action name="hiveAction" cred="someCredentials">
        |        <hive xmlns="uri:oozie:hive-action:0.5">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${hiveAction_jobXml}</job-xml>
        |            <script>${hiveAction_scriptName}</script>
        |            <file>${hiveAction_scriptLocation}</file>
        |        </hive>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${hiveAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${hiveAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${hiveAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${hiveAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${hiveAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${hiveAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${hiveAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${hiveAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="sparkAction" cred="someCredentials">
        |        <spark xmlns="uri:oozie:spark-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <prepare>
        |                <delete path="${sparkAction_prepare_delete}"/>
        |                <mkdir path="${sparkAction_prepare_makedir}"/>
        |            </prepare>
        |            <job-xml>${sparkAction_jobXml}</job-xml>
        |            <master>${sparkAction_sparkMasterURL}</master>
        |            <mode>${sparkAction_sparkMode}</mode>
        |            <name>${sparkAction_sparkJobName}</name>
        |            <class>${sparkAction_mainClass}</class>
        |            <jar>${sparkAction_sparkJar}</jar>
        |            <spark-opts>${sparkAction_sparkOptions}</spark-opts>
        |        </spark>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${sparkAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${sparkAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${sparkAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${sparkAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${sparkAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${sparkAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${sparkAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${sparkAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="shellAction" cred="someCredentials">
        |        <shell xmlns="uri:oozie:shell-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <exec>${shellAction_scriptName}</exec>
        |            <file>${shellAction_scriptLocation}#${shellAction_scriptName}</file>
        |        </shell>
        |        <ok to="fsAction"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${shellAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${shellAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${shellAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${shellAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${shellAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${shellAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${shellAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${shellAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="fsAction">
        |        <fs>
        |            <job-xml>${fsAction_jobXml}</job-xml>
        |            <delete path="${fsAction_deletePath0}"/>
        |            <mkdir path="${fsAction_mkDirPath1}"/>
        |            <touchz path="${fsAction_touchzPath2}"/>
        |            <chmod 
        |            path="${fsAction_chmodPath3}" permissions="${fsAction_chmodPermissions3}" dir-files="${fsAction_chmodDirFiles3}">
        |</chmod>
        |            <move source="${fsAction_moveSrcPath4}" target="${fsAction_moveTargetPath4}"/>
        |        </fs>
        |        <ok to="distCPAction"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${fsAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${fsAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${fsAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${fsAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${fsAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${fsAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${fsAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${fsAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="distCPAction" cred="someCredentials">
        |        <distcp xmlns="uri:oozie:distcp-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <java-opts>${distCPAction_javaOptions}</java-opts>
        |            <arg>${distCPAction_arguments0}</arg>
        |            <arg>${distCPAction_arguments1}</arg>
        |            <arg>${distCPAction_arguments2}</arg>
        |        </distcp>
        |        <ok to="javaAction"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${distCPAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${distCPAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${distCPAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${distCPAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${distCPAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${distCPAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${distCPAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${distCPAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="javaAction" cred="someCredentials">
        |        <java>
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <main-class>${javaAction_mainClass}</main-class>
        |            <java-opts>${javaAction_javaOptions}</java-opts>
        |            <arg>${javaAction_commandLineArg0}</arg>
        |            <file>${javaAction_files0}</file>
        |            <file>${javaAction_files1}</file>
        |            <file>${javaAction_javaJar}</file>
        |        </java>
        |        <ok to="pigAction"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${javaAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${javaAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${javaAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${javaAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${javaAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${javaAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${javaAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${javaAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="pigAction" cred="someCredentials">
        |        <pig>
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <script>${pigAction_script}</script>
        |            <param>${pigAction_param0}</param>
        |            <param>${pigAction_param1}</param>
        |        </pig>
        |        <ok to="sqoopFork"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${pigAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${pigAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${pigAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${pigAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${pigAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${pigAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${pigAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${pigAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <fork name="sqoopFork">
        |        <path start="sqoopAction1"/>
        |        <path start="sqoopAction2"/>
        |    </fork>
        |    <action name="sqoopAction1" cred="someCredentials">
        |        <sqoop xmlns="uri:oozie:sqoop-action:0.3">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <command>${sqoopAction1_command}</command>
        |        </sqoop>
        |        <ok to="sqoopJoin"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${sqoopAction1_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${sqoopAction1_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${sqoopAction1_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${sqoopAction1_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${sqoopAction1_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${sqoopAction1_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${sqoopAction1_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${sqoopAction1_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="sqoopAction2" cred="someCredentials">
        |        <sqoop xmlns="uri:oozie:sqoop-action:0.3">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <arg>${sqoopAction2_arguments0}</arg>
        |            <arg>${sqoopAction2_arguments1}</arg>
        |        </sqoop>
        |        <ok to="sqoopJoin"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${sqoopAction2_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${sqoopAction2_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${sqoopAction2_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${sqoopAction2_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${sqoopAction2_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${sqoopAction2_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${sqoopAction2_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${sqoopAction2_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <join name="sqoopJoin" to="sshAction"/>
        |    <action name="sshAction">
        |        <ssh xmlns="uri:oozie:ssh-action:0.2">
        |            <host>${sshAction_host}</host>
        |            <command>${sshAction_command}</command>
        |            <args>${sshAction_arg0}</args>
        |            <args>${sshAction_arg1}</args>
        |            <capture-output/>
        |        </ssh>
        |        <ok to="subworkflowAction"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${sshAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${sshAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${sshAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${sshAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${sshAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${sshAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${sshAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${sshAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="subworkflowAction" cred="someCredentials">
        |        <sub-workflow>
        |            <app-path>${subworkflowAction_applicationPath}</app-path>
        |            <propagate-configuration/>
        |        </sub-workflow>
        |        <ok to="end"/>
        |        <error to="errorEmail"/>
        |        <sla:info>
        |            <sla:nominal-time>${subworkflowAction_sla_nominalTime}</sla:nominal-time>
        |            <sla:should-start>${subworkflowAction_sla_shouldStart}</sla:should-start>
        |            <sla:should-end>${subworkflowAction_sla_shouldStart}</sla:should-end>
        |            <sla:max-duration>${subworkflowAction_sla_maxDuration}</sla:max-duration>
        |            <sla:alert-events>${subworkflowAction_sla_alertEvents}</sla:alert-events>
        |            <sla:alert-contact>${subworkflowAction_sla_alertContacts}</sla:alert-contact>
        |            <sla:notification-msg>${subworkflowAction_sla_notificationMsg}</sla:notification-msg>
        |            <sla:upstream-apps>${subworkflowAction_sla_upstreamApps}</sla:upstream-apps>
        |        </sla:info>
        |    </action>
        |    <action name="errorEmail">
        |        <email xmlns="uri:oozie:email-action:0.2">
        |            <to>${errorEmail_to}</to>
        |            <subject>${errorEmail_subject}</subject>
        |            <body>${errorEmail_body}</body>
        |        </email>
        |        <ok to="kill"/>
        |        <error to="kill"/>
        |    </action>
        |    <kill name="kill">
        |        <message>workflow is kill</message>
        |    </kill>
        |    <end name="end"/>
        |    <sla:info>
        |        <sla:nominal-time>${someworkflow_sla_nominalTime}</sla:nominal-time>
        |        <sla:should-start>${someworkflow_sla_shouldStart}</sla:should-start>
        |        <sla:should-end>${someworkflow_sla_shouldStart}</sla:should-end>
        |        <sla:max-duration>${someworkflow_sla_maxDuration}</sla:max-duration>
        |        <sla:alert-events>${someworkflow_sla_alertEvents}</sla:alert-events>
        |        <sla:alert-contact>${someworkflow_sla_alertContacts}</sla:alert-contact>
        |        <sla:notification-msg>${someworkflow_sla_notificationMsg}</sla:notification-msg>
        |        <sla:upstream-apps>${someworkflow_sla_upstreamApps}</sla:upstream-apps>
        |    </sla:info>
        |</workflow-app>""".stripMargin
    )

    ouputProperties.lines.mkString(System.lineSeparator()) should be(
      """someCoordinator_end=someEnd
        |someCoordinator_frequency=someFreq
        |someCoordinator_property0="value1"
        |someCoordinator_property1="value2"
        |someCoordinator_property2="value3"
        |someCoordinator_property3="value4"
        |someCoordinator_sla_alertContacts=a@a.com,b@b.com
        |someCoordinator_sla_alertEvents=start_miss,end_miss,duration_miss
        |someCoordinator_sla_maxDuration=120 * MINUTES
        |someCoordinator_sla_nominalTime=nominalTime
        |someCoordinator_sla_notificationMsg=someworkflow is breaching SLA
        |someCoordinator_sla_shouldEnd=120 * MINUTES
        |someCoordinator_sla_shouldStart=10 * MINUTES
        |someCoordinator_sla_upstreamApps=app1,app2
        |someCoordinator_start=someStart
        |someCoordinator_timezone=someTimezone
        |someCoordinator_workflow_path=somepath
        |distCPAction_arguments0=one
        |distCPAction_arguments1=two
        |distCPAction_arguments2=three
        |distCPAction_javaOptions=-Dno.bugs=true
        |distCPAction_sla_alertContacts=a@a.com,b@b.com
        |distCPAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |distCPAction_sla_maxDuration=30 * MINUTES
        |distCPAction_sla_nominalTime=nominalTime
        |distCPAction_sla_notificationMsg=someworkflow is breaching SLA
        |distCPAction_sla_shouldEnd=30 * MINUTES
        |distCPAction_sla_shouldStart=10 * MINUTES
        |distCPAction_sla_upstreamApps=app1,app2
        |errorEmail_body=yep
        |errorEmail_subject=hello
        |errorEmail_to=a@a.com
        |fsAction_chmodDirFiles3=true
        |fsAction_chmodPath3=/some/chmod/dir
        |fsAction_chmodPermissions3=0755
        |fsAction_deletePath0=/some/delte/dir
        |fsAction_jobXml=/path/to/job.xml
        |fsAction_mkDirPath1=/some/make/dir
        |fsAction_moveSrcPath4=/some/move/source
        |fsAction_moveTargetPath4=/some/move/target
        |fsAction_sla_alertContacts=a@a.com,b@b.com
        |fsAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |fsAction_sla_maxDuration=30 * MINUTES
        |fsAction_sla_nominalTime=nominalTime
        |fsAction_sla_notificationMsg=someworkflow is breaching SLA
        |fsAction_sla_shouldEnd=30 * MINUTES
        |fsAction_sla_shouldStart=10 * MINUTES
        |fsAction_sla_upstreamApps=app1,app2
        |fsAction_touchzPath2=/some/touch/dir
        |hiveAction_jobXml=settings
        |hiveAction_scriptLocation=/some/location
        |hiveAction_scriptName=script.hql
        |hiveAction_sla_alertContacts=a@a.com,b@b.com
        |hiveAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |hiveAction_sla_maxDuration=30 * MINUTES
        |hiveAction_sla_nominalTime=nominalTime
        |hiveAction_sla_notificationMsg=someworkflow is breaching SLA
        |hiveAction_sla_shouldEnd=30 * MINUTES
        |hiveAction_sla_shouldStart=10 * MINUTES
        |hiveAction_sla_upstreamApps=app1,app2
        |javaAction_commandLineArg0=somthing
        |javaAction_files0=file1
        |javaAction_files1=file2
        |javaAction_javaJar=some.jar
        |javaAction_javaOptions=-Dno.bugs=true
        |javaAction_mainClass=some.main.class
        |javaAction_sla_alertContacts=a@a.com,b@b.com
        |javaAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |javaAction_sla_maxDuration=30 * MINUTES
        |javaAction_sla_nominalTime=nominalTime
        |javaAction_sla_notificationMsg=someworkflow is breaching SLA
        |javaAction_sla_shouldEnd=30 * MINUTES
        |javaAction_sla_shouldStart=10 * MINUTES
        |javaAction_sla_upstreamApps=app1,app2
        |jobTracker=someNameNode
        |nameNode=someJobTracker
        |pigAction_param0=one
        |pigAction_param1=two
        |pigAction_script=
        |pigAction_sla_alertContacts=a@a.com,b@b.com
        |pigAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |pigAction_sla_maxDuration=30 * MINUTES
        |pigAction_sla_nominalTime=nominalTime
        |pigAction_sla_notificationMsg=someworkflow is breaching SLA
        |pigAction_sla_shouldEnd=30 * MINUTES
        |pigAction_sla_shouldStart=10 * MINUTES
        |pigAction_sla_upstreamApps=app1,app2
        |shellAction_scriptLocation=/some/location
        |shellAction_scriptName=script.sh
        |shellAction_sla_alertContacts=a@a.com,b@b.com
        |shellAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |shellAction_sla_maxDuration=30 * MINUTES
        |shellAction_sla_nominalTime=nominalTime
        |shellAction_sla_notificationMsg=someworkflow is breaching SLA
        |shellAction_sla_shouldEnd=30 * MINUTES
        |shellAction_sla_shouldStart=10 * MINUTES
        |shellAction_sla_upstreamApps=app1,app2
        |someworkflow_credentialProperty0="value1"
        |someworkflow_credentialProperty1="value2"
        |someworkflow_credentialProperty2="value3"
        |someworkflow_credentialProperty3="value4"
        |someworkflow_property0="value1"
        |someworkflow_property1="value2"
        |someworkflow_property2="value3"
        |someworkflow_property3="value4"
        |someworkflow_sla_alertContacts=a@a.com,b@b.com
        |someworkflow_sla_alertEvents=start_miss,end_miss,duration_miss
        |someworkflow_sla_maxDuration=120 * MINUTES
        |someworkflow_sla_nominalTime=nominalTime
        |someworkflow_sla_notificationMsg=someworkflow is breaching SLA
        |someworkflow_sla_shouldEnd=120 * MINUTES
        |someworkflow_sla_shouldStart=10 * MINUTES
        |someworkflow_sla_upstreamApps=app1,app2
        |sparkAction_jobXml=someSettings
        |sparkAction_mainClass=somemainclass
        |sparkAction_prepare_delete="deletePath"
        |sparkAction_prepare_makedir="makePath"
        |sparkAction_sla_alertContacts=a@a.com,b@b.com
        |sparkAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |sparkAction_sla_maxDuration=30 * MINUTES
        |sparkAction_sla_nominalTime=nominalTime
        |sparkAction_sla_notificationMsg=someworkflow is breaching SLA
        |sparkAction_sla_shouldEnd=30 * MINUTES
        |sparkAction_sla_shouldStart=10 * MINUTES
        |sparkAction_sla_upstreamApps=app1,app2
        |sparkAction_sparkJar=spark.jar
        |sparkAction_sparkJobName=Jobname
        |sparkAction_sparkMasterURL=masterurl
        |sparkAction_sparkMode=mode
        |sparkAction_sparkOptions=spark-options
        |sqoopAction1_command=some command
        |sqoopAction1_sla_alertContacts=a@a.com,b@b.com
        |sqoopAction1_sla_alertEvents=start_miss,end_miss,duration_miss
        |sqoopAction1_sla_maxDuration=30 * MINUTES
        |sqoopAction1_sla_nominalTime=nominalTime
        |sqoopAction1_sla_notificationMsg=someworkflow is breaching SLA
        |sqoopAction1_sla_shouldEnd=30 * MINUTES
        |sqoopAction1_sla_shouldStart=10 * MINUTES
        |sqoopAction1_sla_upstreamApps=app1,app2
        |sqoopAction2_arguments0=one
        |sqoopAction2_arguments1=two
        |sqoopAction2_sla_alertContacts=a@a.com,b@b.com
        |sqoopAction2_sla_alertEvents=start_miss,end_miss,duration_miss
        |sqoopAction2_sla_maxDuration=30 * MINUTES
        |sqoopAction2_sla_nominalTime=nominalTime
        |sqoopAction2_sla_notificationMsg=someworkflow is breaching SLA
        |sqoopAction2_sla_shouldEnd=30 * MINUTES
        |sqoopAction2_sla_shouldStart=10 * MINUTES
        |sqoopAction2_sla_upstreamApps=app1,app2
        |sshAction_arg0=ssh1
        |sshAction_arg1=ssh2
        |sshAction_command=someCommand
        |sshAction_host=someHost
        |sshAction_sla_alertContacts=a@a.com,b@b.com
        |sshAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |sshAction_sla_maxDuration=30 * MINUTES
        |sshAction_sla_nominalTime=nominalTime
        |sshAction_sla_notificationMsg=someworkflow is breaching SLA
        |sshAction_sla_shouldEnd=30 * MINUTES
        |sshAction_sla_shouldStart=10 * MINUTES
        |sshAction_sla_upstreamApps=app1,app2
        |subworkflowAction_applicationPath=/some/app/path
        |subworkflowAction_sla_alertContacts=a@a.com,b@b.com
        |subworkflowAction_sla_alertEvents=start_miss,end_miss,duration_miss
        |subworkflowAction_sla_maxDuration=30 * MINUTES
        |subworkflowAction_sla_nominalTime=nominalTime
        |subworkflowAction_sla_notificationMsg=someworkflow is breaching SLA
        |subworkflowAction_sla_shouldEnd=30 * MINUTES
        |subworkflowAction_sla_shouldStart=10 * MINUTES
        |subworkflowAction_sla_upstreamApps=app1,app2""".stripMargin
    )

  }
}
