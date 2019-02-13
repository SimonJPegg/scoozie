package org.antipathy.scoozie.builder

import com.typesafe.config.ConfigFactory
import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.exception.InvalidConfigurationException
import org.scalatest.{FlatSpec, Matchers}

import scala.util.control.NonFatal

class WorkflowBuilderSpec extends FlatSpec with Matchers {

  behavior of "WorkflowBuilder"

  it should "build a workflow with a fork" in {

    val config = ConfigFactory.parseString("""workflow {
                                             |    name: "someworkflow"
                                             |    path: "somepath"
                                             |    credentials {
                                             |      name: "someCredentials"
                                             |      type: "credentialsType"
                                             |      configuration: {
                                             |        credentials1:"value1",
                                             |        credentials2:"value2",
                                             |        credentials3:"value3",
                                             |        credentials4:"value4"
                                             |      }
                                             |    }
                                             |    transitions: [
                                             |      {
                                             |        type:"start"
                                             |        ok-to:"mainFork"
                                             |      },
                                             |      {
                                             |        name:"mainFork"
                                             |        type:"fork"
                                             |        paths: ["sparkAction", "hiveAction"]
                                             |      },
                                             |      {
                                             |        name:"sparkAction"
                                             |        type:"spark"
                                             |        job-xml: "someSettings"
                                             |        spark-master-url: "masterurl"
                                             |        spark-mode: "mode"
                                             |        spark-job-name: "Jobname"
                                             |        main-class: "somemainclass"
                                             |        spark-jar: "spark.jar"
                                             |        spark-options: "spark-options"
                                             |        command-line-arguments: []
                                             |        files: []
                                             |        configuration: {}
                                             |        ok-to: "mainJoin"
                                             |        error-to: "errorEmail"
                                             |      },
                                             |      {
                                             |        name:"hiveAction"
                                             |        type: "hive"
                                             |        job-xml: "settings"
                                             |        script-name: "script.hql"
                                             |        script-location: "/some/location"
                                             |        parameters: []
                                             |        files: []
                                             |        configuration: {}
                                             |        ok-to: "mainJoin"
                                             |        error-to: "errorEmail"
                                             |      },
                                             |      {
                                             |        name:"mainJoin"
                                             |        type:"join"
                                             |        ok-to: "shellAction"
                                             |      },
                                             |      {
                                             |        name:"shellAction"
                                             |        type:"shell"
                                             |        script-name: "script.sh"
                                             |        script-location: "/some/location"
                                             |        command-line-arguments: []
                                             |        environment-variables: []
                                             |        files: []
                                             |        configuration: {}
                                             |        ok-to: "end"
                                             |        error-to: "errorEmail"
                                             |      },
                                             |      {
                                             |        name:"errorEmail"
                                             |        type:"email"
                                             |        to: ["a@a.com"]
                                             |        cc: []
                                             |        subject: "hello"
                                             |        body: "yep"
                                             |        ok-to: "kill"
                                             |        error-to: "kill"
                                             |      },
                                             |      {
                                             |        type: "kill"
                                             |        message: "workflow is kill"
                                             |      },
                                             |      {
                                             |        type:"end"
                                             |      }
                                             |    ]
                                             |    configuration: {
                                             |      workflow1:"value1",
                                             |      workflow2:"value2",
                                             |      workflow3:"value3",
                                             |      workflow4:"value4"
                                             |    }
                                             |    yarn-config {
                                             |      name-node: "someNameNode"
                                             |      job-tracker: "someJobTracker"
                                             |    }
                                             |}
                                             |""".stripMargin)

    val result = WorkflowBuilder.build(config)

    scala.xml.Utility.trim(result.toXML) should be(
      scala.xml.Utility
        .trim(<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="someworkflow">
          <global>
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <configuration>
              <property>
                <name>workflow1</name>
                <value>{"${someworkflow_property0}"}</value>
              </property>
              <property>
                <name>workflow2</name>
                <value>{"${someworkflow_property1}"}</value>
              </property>
              <property>
                <name>workflow3</name>
                <value>{"${someworkflow_property2}"}</value>
              </property>
              <property>
                <name>workflow4</name>
                <value>{"${someworkflow_property3}"}</value>
              </property>
            </configuration>
          </global>

          <credentials>
            <credential name="someCredentials" type="credentialsType">
              <property>
                <name>credentials1</name>
                <value>{"${someworkflow_credentialProperty0}"}</value>
              </property>
              <property>
                <name>credentials2</name>
                <value>{"${someworkflow_credentialProperty1}"}</value>
              </property>
              <property>
                <name>credentials3</name>
                <value>{"${someworkflow_credentialProperty2}"}</value>
              </property>
              <property>
                <name>credentials4</name>
                <value>{"${someworkflow_credentialProperty3}"}</value>
              </property>
            </credential>
          </credentials>

          <start to="mainFork" />

          <fork name="mainFork">
            <path start="sparkAction" />
            <path start="hiveAction" />
          </fork>

          <action name="sparkAction" cred="someCredentials">
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
            <error to="errorEmail" />
          </action>

          <action name="hiveAction" cred="someCredentials">
            <hive xmlns="uri:oozie:hive-action:0.5">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${hiveAction_jobXml}"}</job-xml>
              <script>{"${hiveAction_scriptName}"}</script>
              <file>{"${hiveAction_scriptLocation}"}</file>
            </hive>
            <ok to="mainJoin" />
            <error to="errorEmail" />
          </action>

          <join name="mainJoin" to="shellAction" />

          <action name="shellAction" cred="someCredentials">
            <shell xmlns="uri:oozie:shell-action:0.1">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <exec>{"${shellAction_scriptName}"}</exec>
              <file>{"${shellAction_scriptLocation}#${shellAction_scriptName}"}</file>
            </shell>
            <ok to="end" />
            <error to="errorEmail" />
          </action>

          <action name="errorEmail">
            <email xmlns="uri:oozie:email-action:0.1">
              <to>{"${errorEmail_to}"}</to>
              <subject>{"${errorEmail_subject}"}</subject>
              <body>{"${errorEmail_body}"}</body>
            </email>
            <ok to="kill" />
            <error to="kill" />
          </action>

          <kill name="kill">
            <message>workflow is kill</message>
          </kill>

          <end name="end" />
        </workflow-app>)
    )

    result.jobProperties should be("""errorEmail_body=yep
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
                                     |sparkAction_sparkJar=spark.jar
                                     |sparkAction_sparkJobName=Jobname
                                     |sparkAction_sparkMasterURL=masterurl
                                     |sparkAction_sparkMode=mode
                                     |sparkAction_sparkOptions=spark-options""".stripMargin)

    Scoozie.Test.validate(result)
  }

  it should "build a workflow with a decision" in {

    val config = ConfigFactory.parseString("""workflow {
                                             |    name: "someworkflow"
                                             |    path: "somepath"
                                             |    credentials {
                                             |      name: "someCredentials"
                                             |      type: "credentialsType"
                                             |      configuration: {
                                             |        credentials1:"value1",
                                             |        credentials2:"value2",
                                             |        credentials3:"value3",
                                             |        credentials4:"value4"
                                             |      }
                                             |    }
                                             |    transitions: [
                                             |      {
                                             |        type:"start"
                                             |        ok-to:"decisionNode"
                                             |      },
                                             |      {
                                             |        name:"decisionNode"
                                             |        type:"decision"
                                             |        default: "sparkAction"
                                             |        switches: {
                                             |          sparkAction: "someVar eq 1"
                                             |          hiveAction: "someOtherVar eq someVar"
                                             |        }
                                             |      }
                                             |      {
                                             |        name:"sparkAction"
                                             |        type:"spark"
                                             |        job-xml: "someSettings"
                                             |        spark-master-url: "masterurl"
                                             |        spark-mode: "mode"
                                             |        spark-job-name: "Jobname"
                                             |        main-class: "somemainclass"
                                             |        spark-jar: "spark.jar"
                                             |        spark-options: "spark-options"
                                             |        command-line-arguments: []
                                             |        prepare: {
                                             |              delete: "deletePath"
                                             |              mkdir: "makePath"
                                             |        }
                                             |        files: []
                                             |        configuration: {}
                                             |        ok-to: "shellAction"
                                             |        error-to: "errorEmail"
                                             |      },
                                             |      {
                                             |        name:"hiveAction"
                                             |        type: "hive"
                                             |        job-xml: "settings"
                                             |        script-name: "script.hql"
                                             |        script-location: "/some/location"
                                             |        parameters: []
                                             |        files: []
                                             |        configuration: {}
                                             |        ok-to: "shellAction"
                                             |        error-to: "errorEmail"
                                             |      },
                                             |      {
                                             |        name:"shellAction"
                                             |        type:"shell"
                                             |        script-name: "script.sh"
                                             |        script-location: "/some/location"
                                             |        command-line-arguments: []
                                             |        environment-variables: []
                                             |        files: []
                                             |        configuration: {}
                                             |        ok-to: "end"
                                             |        error-to: "errorEmail"
                                             |      },
                                             |      {
                                             |        name:"errorEmail"
                                             |        type:"email"
                                             |        to: ["a@a.com"]
                                             |        cc: []
                                             |        subject: "hello"
                                             |        body: "yep"
                                             |        ok-to: "kill"
                                             |        error-to: "kill"
                                             |      },
                                             |      {
                                             |        type: "kill"
                                             |        message: "workflow is kill"
                                             |      },
                                             |      {
                                             |        type:"end"
                                             |      }
                                             |    ]
                                             |    configuration: {
                                             |      workflow1:"value1",
                                             |      workflow2:"value2",
                                             |      workflow3:"value3",
                                             |      workflow4:"value4"
                                             |    }
                                             |    yarn-config {
                                             |      name-node: "someNameNode"
                                             |      job-tracker: "someJobTracker"
                                             |    }
                                             |}""".stripMargin)

    val result = WorkflowBuilder.build(config)

    scala.xml.Utility.trim(result.toXML) should be(
      scala.xml.Utility
        .trim(<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="someworkflow">

          <global>
            <job-tracker>{"${jobTracker}"}</job-tracker>
            <name-node>{"${nameNode}"}</name-node>
            <configuration>
              <property>
                <name>workflow1</name>
                <value>{"${someworkflow_property0}"}</value>
              </property>
              <property>
                <name>workflow2</name>
                <value>{"${someworkflow_property1}"}</value>
              </property>
              <property>
                <name>workflow3</name>
                <value>{"${someworkflow_property2}"}</value>
              </property>
              <property>
                <name>workflow4</name>
                <value>{"${someworkflow_property3}"}</value>
              </property>
            </configuration>
          </global>

          <credentials>
            <credential name="someCredentials" type="credentialsType">
              <property>
                <name>credentials1</name>
                <value>{"${someworkflow_credentialProperty0}"}</value>
              </property>
              <property>
                <name>credentials2</name>
                <value>{"${someworkflow_credentialProperty1}"}</value>
              </property>
              <property>
                <name>credentials3</name>
                <value>{"${someworkflow_credentialProperty2}"}</value>
              </property>
              <property>
                <name>credentials4</name>
                <value>{"${someworkflow_credentialProperty3}"}</value>
              </property>
            </credential>
          </credentials>

          <start to="decisionNode" />

          <decision name="decisionNode">
            <switch>
              <case to="hiveAction">{"${someOtherVar eq someVar}"}</case>
              <case to="sparkAction">{"${someVar eq 1}"}</case>
              <default to="sparkAction" />
            </switch>
          </decision>

          <action name="hiveAction" cred="someCredentials">
            <hive xmlns="uri:oozie:hive-action:0.5">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <job-xml>{"${hiveAction_jobXml}"}</job-xml>
              <script>{"${hiveAction_scriptName}"}</script>
              <file>{"${hiveAction_scriptLocation}"}</file>
            </hive>
            <ok to="shellAction" />
            <error to="errorEmail" />
          </action>

          <action name="sparkAction" cred="someCredentials">
            <spark xmlns="uri:oozie:spark-action:1.0">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <prepare>
                <delete path="${sparkAction_prepare_delete}"/>
                <mkdir path="${sparkAction_prepare_makedir}"/>
              </prepare>
              <job-xml>{"${sparkAction_jobXml}"}</job-xml>
              <master>{"${sparkAction_sparkMasterURL}"}</master>
              <mode>{"${sparkAction_sparkMode}"}</mode>
              <name>{"${sparkAction_sparkJobName}"}</name>
              <class>{"${sparkAction_mainClass}"}</class>
              <jar>{"${sparkAction_sparkJar}"}</jar>
              <spark-opts>{"${sparkAction_sparkOptions}"}</spark-opts>
            </spark>
            <ok to="shellAction" />
            <error to="errorEmail" />
          </action>

          <action name="shellAction" cred="someCredentials">
            <shell xmlns="uri:oozie:shell-action:0.1">
              <job-tracker>{"${jobTracker}"}</job-tracker>
              <name-node>{"${nameNode}"}</name-node>
              <exec>{"${shellAction_scriptName}"}</exec>
              <file>{"${shellAction_scriptLocation}#${shellAction_scriptName}"}</file>
            </shell>
            <ok to="end" />
            <error to="errorEmail" />
          </action>

          <action name="errorEmail">
            <email xmlns="uri:oozie:email-action:0.1">
              <to>{"${errorEmail_to}"}</to>
              <subject>{"${errorEmail_subject}"}</subject>
              <body>{"${errorEmail_body}"}</body>
            </email>
            <ok to="kill" />
            <error to="kill" />
          </action>

          <kill name="kill">
            <message>workflow is kill</message>
          </kill>

          <end name="end" />
      </workflow-app>)
    )

    result.jobProperties should be("""errorEmail_body=yep
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
                                     |sparkAction_sparkOptions=spark-options""".stripMargin)

    Scoozie.Test.validate(result)
  }

  it should "raise an error when a configuration item is missing" in {

    val config = ConfigFactory.parseString("""workflow {
                                             |  name: "someworkflow"
                                             |  path: "SomePath"
                                             |  credentials {
                                             |    name: "someCredentials"
                                             |    type: "credentialsType"
                                             |    configuration: {
                                             |      credentials1:"value1",
                                             |      credentials2:"value2",
                                             |      credentials3:"value3",
                                             |      credentials4:"value4"
                                             |    }
                                             |  }
                                             |  transitions: [
                                             |    {
                                             |      type:"start"
                                             |      ok-to:"decisionNode"
                                             |    },
                                             |    {
                                             |      name:"decisionNode"
                                             |      type:"decision"
                                             |      default: "sparkAction"
                                             |      switches: {
                                             |        sparkAction: "someVar eq 1"
                                             |        hiveAction: "someOtherVar eq someVar"
                                             |      }
                                             |    }
                                             |    {
                                             |      name:"sparkAction"
                                             |      type:"spark"
                                             |      job-xml: "someSettings"
                                             |      spark-mode: "mode"
                                             |      spark-job-name: "Jobname"
                                             |      main-class: "somemainclass"
                                             |      spark-jar: "spark.jar"
                                             |      spark-options: "spark-options"
                                             |      command-line-arguments: []
                                             |      prepare: {
                                             |        delete: "deletePath"
                                             |        mkdir: "makePath"
                                             |      }
                                             |      files: []
                                             |      configuration: {}
                                             |      ok-to: "shellAction"
                                             |      error-to: "errorEmail"
                                             |    },
                                             |    {
                                             |      name:"hiveAction"
                                             |      type: "hive"
                                             |      job-xml: "settings"
                                             |      script-name: "script.hql"
                                             |      script-location: "/some/location"
                                             |      parameters: []
                                             |      files: []
                                             |      configuration: {}
                                             |      ok-to: "shellAction"
                                             |      error-to: "errorEmail"
                                             |    },
                                             |    {
                                             |      name:"shellAction"
                                             |      type:"shell"
                                             |      script-name: "script.sh"
                                             |      script-location: "/some/location"
                                             |      command-line-arguments: []
                                             |      environment-variables: []
                                             |      files: []
                                             |      configuration: {}
                                             |      ok-to: "end"
                                             |      error-to: "errorEmail"
                                             |    },
                                             |    {
                                             |      name:"errorEmail"
                                             |      type:"email"
                                             |      to: ["a@a.com"]
                                             |      cc: []
                                             |      subject: "hello"
                                             |      body: "yep"
                                             |      ok-to: "kill"
                                             |      error-to: "kill"
                                             |    },
                                             |    {
                                             |      type: "kill"
                                             |      message: "workflow is kill"
                                             |    },
                                             |    {
                                             |      type:"end"
                                             |    }
                                             |  ]
                                             |  configuration: {
                                             |    workflow1:"value1",
                                             |    workflow2:"value2",
                                             |    workflow3:"value3",
                                             |    workflow4:"value4"
                                             |  }
                                             |  yarn-config {
                                             |    name-node: "someNameNode"
                                             |    job-tracker: "someJobTracker"
                                             |  }
                                             |}
                                             |coordinator: {
                                             |  name: "someCoordinator"
                                             |  frequency: "someFreq"
                                             |  start: "someStart"
                                             |  end: "someEnd"
                                             |  timezone: "someTimezone"
                                             |  configuration: {
                                             |    prop1: value1
                                             |    prop2: value2
                                             |    prop3: value3
                                             |    prop4: value4
                                             |  }
                                             |}
                                             |validate {
                                             |  transitions = "start -> sparkAction -> end"
                                             |}""".stripMargin)

    an[InvalidConfigurationException] should be thrownBy {
      try {
        WorkflowBuilder.build(config)
      } catch {
        case NonFatal(e) =>
          e.getMessage should be("No configuration setting found for key 'spark-master-url' in sparkAction in workflow")
          throw e
      }
    }
  }
}
