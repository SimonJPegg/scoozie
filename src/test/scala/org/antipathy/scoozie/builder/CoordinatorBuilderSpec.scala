package org.antipathy.scoozie.builder

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class CoordinatorBuilderSpec extends FlatSpec with Matchers {

  behavior of "CoordinatorBuilder"

  it should "build a coordinator" in {
    import org.antipathy.scoozie.Scoozie

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
                                             |        spark-settings: "someSettings"
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
                                             |
                                             |coordinator: {
                                             | name: "someCoordinator"
                                             | path : "somePath"
                                             | frequency: "someFreq"
                                             | start: "someStart"
                                             | end: "someEnd"
                                             | timezone: "someTimezone"
                                             | configuration: {
                                             |   prop1: value1
                                             |   prop2: value2
                                             |   prop3: value3
                                             |   prop4: value4
                                             | }
                                             |}""".stripMargin)

    val result = CoordinatorBuilder.build(config)

    result.name should be("someCoordinator")
    result.frequency should be("someFreq")
    result.start should be("someStart")
    result.end should be("someEnd")
    result.timezone should be("someTimezone")
    result.configuration.properties should be(
      Map("prop1" -> "\"value1\"", "prop2" -> "\"value2\"", "prop3" -> "\"value3\"", "prop4" -> "\"value4\"")
    )
    result.workflow.path should be("somepath")

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<coordinator-app name="someCoordinator"
                       frequency={"${someFreq}"}
                       start={"${someCoordinator_start}"}
                       end={"${someCoordinator_end}"}
                       timezone={"${someCoordinator_timezone}"}
                       xmlns="uri:oozie:coordinator:0.4" xmlns:sla="uri:oozie:sla:0.2">
        <action>
          <workflow>
            <app-path>{"${someCoordinator_workflow_path}"}</app-path>
            <configuration>
              <property>
                <name>prop1</name>
                <value>{"${someCoordinator_property0}"}</value>
              </property>
              <property>
                <name>prop2</name>
                <value>{"${someCoordinator_property1}"}</value>
              </property>
              <property>
                <name>prop3</name>
                <value>{"${someCoordinator_property2}"}</value>
              </property>
              <property>
                <name>prop4</name>
                <value>{"${someCoordinator_property3}"}</value>
              </property>
            </configuration>
          </workflow>
        </action>
      </coordinator-app>))

    result.jobProperties should be("""someCoordinator_end=someEnd
                                     |someCoordinator_property0=value1
                                     |someCoordinator_property1=value2
                                     |someCoordinator_property2=value3
                                     |someCoordinator_property3=value4
                                     |someCoordinator_start=someStart
                                     |someCoordinator_timezone=someTimezone
                                     |someCoordinator_workflow_path=somepath
                                     |oozie.coord.application.path=somePath/coordinator.xml""".stripMargin)

    Scoozie.Test.validate(result)
  }
}
