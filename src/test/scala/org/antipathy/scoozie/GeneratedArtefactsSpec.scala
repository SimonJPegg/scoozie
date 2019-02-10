package org.antipathy.scoozie

import org.scalatest.{FlatSpec, Matchers}
import better.files._
import org.antipathy.scoozie.io.ArtefactWriter
import scala.util.control.NonFatal
import org.antipathy.scoozie.exception.TransitionException

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
      """<workflow-app name="someworkflow" xmlns="uri:oozie:workflow:0.4">
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
        |        <hive xmlns="uri:oozie:hive-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${hiveAction_hiveSettingsXML}</job-xml>
        |            <script>${hiveAction_scriptName}</script>
        |            <file>${hiveAction_scriptLocation}</file>
        |        </hive>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="sparkAction" cred="someCredentials">
        |        <spark xmlns="uri:oozie:spark-action:1.0">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <prepare>
        |                <delete path="${sparkAction_prepare_delete}"/>
        |                <mkdir path="${sparkAction_prepare_makedir}"/>
        |            </prepare>
        |            <job-xml>${sparkAction_sparkSettings}</job-xml>
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
        |        <shell xmlns="uri:oozie:shell-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <exec>${shellAction_scriptName}</exec>
        |            <file>${shellAction_scriptLocation}#${shellAction_scriptName}</file>
        |        </shell>
        |        <ok to="end"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="errorEmail">
        |        <email xmlns="uri:oozie:email-action:0.1">
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
        |hiveAction_hiveSettingsXML=settings
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
        |sparkAction_mainClass=somemainclass
        |sparkAction_prepare_delete="deletePath"
        |sparkAction_prepare_makedir="makePath"
        |sparkAction_sparkJar=spark.jar
        |sparkAction_sparkJobName=Jobname
        |sparkAction_sparkMasterURL=masterurl
        |sparkAction_sparkMode=mode
        |sparkAction_sparkOptions=spark-options
        |sparkAction_sparkSettings=someSettings""".stripMargin
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
      """name="someCoordinator" frequency="someFreq" start="someStart" end="someEnd" timezone="someTimezone" xmlns="uri:oozie:coordinator:0.4">
        |    <action>
        |        <workflow>
        |            <app-path>somepath</app-path>
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
      """<workflow-app name="someworkflow" xmlns="uri:oozie:workflow:0.4">
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
        |        <hive xmlns="uri:oozie:hive-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${hiveAction_hiveSettingsXML}</job-xml>
        |            <script>${hiveAction_scriptName}</script>
        |            <file>${hiveAction_scriptLocation}</file>
        |        </hive>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="sparkAction" cred="someCredentials">
        |        <spark xmlns="uri:oozie:spark-action:1.0">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <prepare>
        |                <delete path="${sparkAction_prepare_delete}"/>
        |                <mkdir path="${sparkAction_prepare_makedir}"/>
        |            </prepare>
        |            <job-xml>${sparkAction_sparkSettings}</job-xml>
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
        |        <shell xmlns="uri:oozie:shell-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <exec>${shellAction_scriptName}</exec>
        |            <file>${shellAction_scriptLocation}#${shellAction_scriptName}</file>
        |        </shell>
        |        <ok to="end"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="errorEmail">
        |        <email xmlns="uri:oozie:email-action:0.1">
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
      """someCoordinator_property0="value1"
        |someCoordinator_property1="value2"
        |someCoordinator_property2="value3"
        |someCoordinator_property3="value4"
        |errorEmail_body=yep
        |errorEmail_subject=hello
        |errorEmail_to=a@a.com
        |hiveAction_hiveSettingsXML=settings
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
        |sparkAction_mainClass=somemainclass
        |sparkAction_prepare_delete="deletePath"
        |sparkAction_prepare_makedir="makePath"
        |sparkAction_sparkJar=spark.jar
        |sparkAction_sparkJobName=Jobname
        |sparkAction_sparkMasterURL=masterurl
        |sparkAction_sparkMode=mode
        |sparkAction_sparkOptions=spark-options
        |sparkAction_sparkSettings=someSettings""".stripMargin
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
      """name="someCoordinator" frequency="someFreq" start="someStart" end="someEnd" timezone="someTimezone" xmlns="uri:oozie:coordinator:0.4">
        |    <action>
        |        <workflow>
        |            <app-path>somepath</app-path>
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
      """<workflow-app name="someworkflow" xmlns="uri:oozie:workflow:0.4">
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
        |        <hive xmlns="uri:oozie:hive-action:0.2">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <job-xml>${hiveAction_hiveSettingsXML}</job-xml>
        |            <script>${hiveAction_scriptName}</script>
        |            <file>${hiveAction_scriptLocation}</file>
        |        </hive>
        |        <ok to="shellAction"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="sparkAction" cred="someCredentials">
        |        <spark xmlns="uri:oozie:spark-action:1.0">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <prepare>
        |                <delete path="${sparkAction_prepare_delete}"/>
        |                <mkdir path="${sparkAction_prepare_makedir}"/>
        |            </prepare>
        |            <job-xml>${sparkAction_sparkSettings}</job-xml>
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
        |        <shell xmlns="uri:oozie:shell-action:0.1">
        |            <job-tracker>${jobTracker}</job-tracker>
        |            <name-node>${nameNode}</name-node>
        |            <exec>${shellAction_scriptName}</exec>
        |            <file>${shellAction_scriptLocation}#${shellAction_scriptName}</file>
        |        </shell>
        |        <ok to="end"/>
        |        <error to="errorEmail"/>
        |    </action>
        |    <action name="errorEmail">
        |        <email xmlns="uri:oozie:email-action:0.1">
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
      """someCoordinator_property0="value1"
        |someCoordinator_property1="value2"
        |someCoordinator_property2="value3"
        |someCoordinator_property3="value4"
        |errorEmail_body=yep
        |errorEmail_subject=hello
        |errorEmail_to=a@a.com
        |hiveAction_hiveSettingsXML=settings
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
        |sparkAction_mainClass=somemainclass
        |sparkAction_prepare_delete="deletePath"
        |sparkAction_prepare_makedir="makePath"
        |sparkAction_sparkJar=spark.jar
        |sparkAction_sparkJobName=Jobname
        |sparkAction_sparkMasterURL=masterurl
        |sparkAction_sparkMode=mode
        |sparkAction_sparkOptions=spark-options
        |sparkAction_sparkSettings=someSettings""".stripMargin
    )

    outputPath.delete()
  }

  it should "raise an error when the transistions do not match the validation string" in {

    val configPath = File("src/test/resources/conf/invalidValidationString.conf").path
    val outputPath = File("src/test/resources/output/generatedArtefacts/invalidValidationString")
    val artefacts = Scoozie.fromConfig(configPath)

    an[TransitionException] should be thrownBy {
      try {
        artefacts.saveToPath(outputPath.path)
      } catch {
        case NonFatal(e) =>
          e.getMessage should be("""Expected transition:
                                   |start -> sparkAction -> end
                                   |Actual transition:
                                   |start -> decisionNode -> sparkAction -> shellAction -> end""".stripMargin)
          throw e
      }
    }
  }
}
