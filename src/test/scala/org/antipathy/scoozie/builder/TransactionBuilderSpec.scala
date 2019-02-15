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
package org.antipathy.scoozie.builder

import com.typesafe.config.ConfigFactory
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.configuration.{Credentials, YarnConfig}
import org.antipathy.scoozie.exception._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.immutable._

class TransactionBuilderSpec extends FlatSpec with Matchers {

  behavior of "TransactionBuilder"

  it should "build a simple workflow" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
                         |      {
                         |        type:"start"
                         |        ok-to:"sparkAction"
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
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)
    val startNode = TransitionBuilder.build(config, yarnConfig)
    startNode.name should be("start")

    startNode.failureTransition should be(None)
    startNode.successTransition.isDefined should be(true)
    startNode.successTransition.foreach { sparkAction =>
      sparkAction.name should be("sparkAction")
      sparkAction.successTransition.get.name should be("shellAction")
      sparkAction.failureTransition.get.name should be("errorEmail")

      sparkAction.successTransition.isDefined should be(true)
      sparkAction.successTransition.foreach { shellAction =>
        shellAction.name should be("shellAction")
        shellAction.successTransition.get.name should be("end")
        shellAction.failureTransition.get.name should be("errorEmail")

        shellAction.successTransition.isDefined should be(true)
        shellAction.successTransition.foreach { endAction =>
          endAction.name should be("end")
          endAction.successTransition should be(None)
          endAction.failureTransition should be(None)
        }
        shellAction.failureTransition.isDefined should be(true)
        shellAction.failureTransition.foreach { emailAction =>
          emailAction.name should be("errorEmail")
          emailAction.successTransition.get.name should be("kill")
          emailAction.failureTransition.get.name should be("kill")
          emailAction.failureTransition.isDefined should be(true)
          emailAction.successTransition.isDefined should be(true)
          emailAction.successTransition.foreach { killAction =>
            killAction.name should be("kill")
            killAction.successTransition should be(None)
            killAction.failureTransition should be(None)
          }
        }
      }
    }
  }

  it should "build a workflow with a fork" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
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
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)
    val startNode = TransitionBuilder.build(config, yarnConfig)
    startNode.name should be("start")
    startNode.failureTransition should be(None)
    startNode.successTransition.foreach { forkNode =>
      forkNode.action.name should be("mainFork")
      forkNode.action.asInstanceOf[Fork].transitionPaths.map(_.name) should be(Seq("sparkAction", "hiveAction"))
      val sparkNode = forkNode.action.asInstanceOf[Fork].transitionPaths.toSet.find(_.name == "sparkAction")
      sparkNode.isDefined should be(true)
      sparkNode.foreach { sparkAction =>
        sparkAction.name should be("sparkAction")
        sparkAction.successTransition.isDefined should be(true)
        sparkAction.successTransition.foreach(_.name should be("mainJoin"))
        sparkAction.failureTransition.isDefined should be(true)
        sparkAction.failureTransition.foreach(_.name should be("errorEmail"))
      }
      val hiveNode = forkNode.action.asInstanceOf[Fork].transitionPaths.toSet.find(_.name == "hiveAction")
      hiveNode.isDefined should be(true)
      hiveNode.foreach { hiveAction =>
        hiveAction.name should be("hiveAction")
        hiveAction.successTransition.isDefined should be(true)
        hiveAction.successTransition.foreach(_.name should be("mainJoin"))
        hiveAction.failureTransition.isDefined should be(true)
        hiveAction.failureTransition.foreach(_.name should be("errorEmail"))
        hiveAction.successTransition.foreach { joinAction =>
          joinAction.name should be("mainJoin")
          joinAction.failureTransition should be(None)
          joinAction.action.asInstanceOf[Join].transitionTo.name should be("shellAction")
          joinAction.successTransition.isDefined should be(true)
          joinAction.successTransition.foreach { shellAction =>
            shellAction.name should be("shellAction")
            shellAction.successTransition.isDefined should be(true)
            shellAction.successTransition.foreach { endAction =>
              endAction.name should be("end")
              endAction.successTransition should be(None)
              endAction.failureTransition should be(None)
            }
            shellAction.failureTransition.isDefined should be(true)
            shellAction.failureTransition.foreach { emailAction =>
              emailAction.name should be("errorEmail")
              emailAction.successTransition.isDefined should be(true)
              emailAction.successTransition.foreach(_.name should be("kill"))
              emailAction.failureTransition.isDefined should be(true)
              emailAction.failureTransition.foreach(_.name should be("kill"))
              emailAction.successTransition.foreach { killAction =>
                killAction.name should be("kill")
                killAction.successTransition should be(None)
                killAction.failureTransition should be(None)
              }
            }
            shellAction.successTransition.get.name should be("end")
            shellAction.failureTransition.get.name should be("errorEmail")
          }
        }
      }
    }

  }

  it should "build a transition with a decision" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
                         |      {
                         |        type:"start"
                         |        ok-to:"decisionNode"
                         |      },
                         |      {
                         |        name:"decisionNode"
                         |        type:"decision"
                         |        default: "sparkAction"
                         |        switches: {
                         |          sparkAction: "${someVar}"
                         |          hiveAction: "${someOtherVar}"
                         |        },
                         |      }
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
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)
    val startNode = TransitionBuilder.build(config, yarnConfig)
    startNode.name should be("start")
    startNode.failureTransition should be(None)

    startNode.successTransition.isDefined should be(true)
    startNode.successTransition.foreach { decisionNode =>
      decisionNode.name should be("decisionNode")
      decisionNode.action.asInstanceOf[Decision].defaultPath.name should be("sparkAction")
      decisionNode.action.asInstanceOf[Decision].transitionPaths.map(_.name).toSet should be(
        Set("sparkAction", "hiveAction")
      )
      decisionNode.successTransition should be(None)
      decisionNode.failureTransition should be(None)
      val sparkNode = decisionNode.action.asInstanceOf[Decision].transitionPaths.toSet.find(_.name == "sparkAction")
      sparkNode.isDefined should be(true)
      sparkNode.foreach { sparkAction =>
        sparkAction.name should be("sparkAction")
        sparkAction.successTransition.isDefined should be(true)
        sparkAction.successTransition.foreach(_.name should be("shellAction"))
        sparkAction.failureTransition.isDefined should be(true)
        sparkAction.failureTransition.foreach(_.name should be("errorEmail"))
        sparkAction.successTransition.foreach { shellAction =>
          shellAction.name should be("shellAction")
          shellAction.successTransition.isDefined should be(true)
          shellAction.successTransition.foreach(_.name should be("end"))
          shellAction.failureTransition.isDefined should be(true)
          shellAction.failureTransition.foreach(_.name should be("errorEmail"))
          shellAction.successTransition.foreach { endAction =>
            endAction.name should be("end")
            endAction.successTransition should be(None)
            endAction.failureTransition should be(None)
          }
        }
      }
      val hiveNode =
        decisionNode.action.asInstanceOf[Decision].transitionPaths.toSet.find(_.name == "hiveAction")
      hiveNode.isDefined should be(true)
      hiveNode.foreach { hiveAction =>
        hiveAction.name should be("hiveAction")
        hiveAction.successTransition.isDefined should be(true)
        hiveAction.successTransition.foreach(_.name should be("shellAction"))
        hiveAction.failureTransition.isDefined should be(true)
        hiveAction.failureTransition.foreach(_.name should be("errorEmail"))
        hiveAction.failureTransition.foreach { emailAction =>
          emailAction.name should be("errorEmail")
          emailAction.successTransition.isDefined should be(true)
          emailAction.successTransition.foreach(_.name should be("kill"))
          emailAction.failureTransition.isDefined should be(true)
          emailAction.failureTransition.foreach(_.name should be("kill"))
          emailAction.successTransition.foreach { killAction =>
            killAction.name should be("kill")
            killAction.successTransition should be(None)
            killAction.failureTransition should be(None)
          }
        }
      }
    }
  }

  it should "raise an error when no default for a decision node is specified" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
                         |      {
                         |        type:"start"
                         |        ok-to:"decisionNode"
                         |      },
                         |      {
                         |        name:"decisionNode"
                         |        type:"decision"
                         |        switches: {
                         |          sparkAction: "${someVar}"
                         |          hiveAction: "${someOtherVar}"
                         |        },
                         |      }
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
                         |        ok-to: "shellAction"
                         |        error-to: "errorEmail"
                         |      },
                         |      {
                         |        name:"hiveAction"
                         |        type: "hive"
                         |        hive-settings-xml: "settings"
                         |        script-name: "script.hql"
                         |        script-location: "/some/location"
                         |        parameters: []
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
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)

    an[ConfigurationMissingException] should be thrownBy {
      TransitionBuilder.build(config, yarnConfig)
    }
  }

  it should "raise an error when a default for a decision node is missing" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
                         |      {
                         |        type:"start"
                         |        ok-to:"decisionNode"
                         |      },
                         |      {
                         |        name:"decisionNode"
                         |        type:"decision"
                         |        default: "sparkAction"
                         |        switches: {
                         |          sparkAction: "${someVar}"
                         |          hiveAction: "${someOtherVar}"
                         |        },
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
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)

    an[TransitionException] should be thrownBy {
      TransitionBuilder.build(config, yarnConfig)
    }
  }

  it should "raise an error when a required node is missing" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
                         |      {
                         |        type:"start"
                         |        ok-to:"sparkAction"
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
                         |        type: "kill"
                         |        message: "workflow is kill"
                         |      },
                         |      {
                         |        type:"end"
                         |      }
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)

    an[ConfigurationMissingException] should be thrownBy {
      TransitionBuilder.build(config, yarnConfig)
    }

  }

  it should "raise an error when a required property is missing" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
                         |      {
                         |        type:"start"
                         |        ok-to:"sparkAction"
                         |      },
                         |      {
                         |        name:"sparkAction"
                         |        type:"spark"
                         |        spark-mode: "mode"
                         |        spark-job-name: "Jobname"
                         |        main-class: "somemainclass"
                         |        spark-jar: "spark.jar"
                         |        spark-options: "spark-options"
                         |        command-line-arguments: []
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
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)

    an[ConfigurationMissingException] should be thrownBy {
      TransitionBuilder.build(config, yarnConfig)
    }

  }

  it should "raise an error when a fork is specified without enough transition paths" in {

    implicit val credentials: Option[Credentials] = None

    val yarnConfig =
      YarnConfig("nameNode", "jobTracker")

    val configString = """transitions: [
                         |      {
                         |        type:"start"
                         |        ok-to:"mainFork"
                         |      },
                         |      {
                         |        name:"mainFork"
                         |        type:"fork"
                         |        paths: ["sparkAction"]
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
                         |    ]""".stripMargin

    val config = Seq(ConfigFactory.parseString(configString).getConfigList("transitions").asScala: _*)

    an[TransitionException] should be thrownBy {
      TransitionBuilder.build(config, yarnConfig)
    }

  }

}
