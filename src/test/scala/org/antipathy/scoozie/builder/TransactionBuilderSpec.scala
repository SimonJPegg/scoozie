package org.antipathy.scoozie.builder

import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.ConfigFactory
import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.configuration.YarnConfig
import scala.collection.JavaConverters._
import scala.collection.immutable._
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.exception._

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

    val sparkAction = startNode.successTransition.get
    sparkAction.name should be("sparkAction")
    sparkAction.successTransition.get.name should be("shellAction")
    sparkAction.failureTransition.get.name should be("errorEmail")

    val shellAction = sparkAction.successTransition.get
    shellAction.name should be("shellAction")
    shellAction.successTransition.get.name should be("end")
    shellAction.failureTransition.get.name should be("errorEmail")

    val emailAction = shellAction.failureTransition.get
    emailAction.name should be("errorEmail")
    emailAction.successTransition.get.name should be("kill")
    emailAction.failureTransition.get.name should be("kill")

    val endAction = shellAction.successTransition.get
    endAction.name should be("end")
    endAction.successTransition should be(None)
    endAction.failureTransition should be(None)

    val killAction = emailAction.successTransition.get
    killAction.name should be("kill")
    killAction.successTransition should be(None)
    killAction.failureTransition should be(None)

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

    val forkNode = startNode.successTransition.get
    forkNode.action.name should be("mainFork")
    forkNode.action.asInstanceOf[Fork].transitionPaths.map(_.name) should be(Seq("sparkAction", "hiveAction"))

    val sparkAction = forkNode.action.asInstanceOf[Fork].transitionPaths.toSet.filter(_.name == "sparkAction").head
    sparkAction.name should be("sparkAction")
    sparkAction.successTransition.get.name should be("mainJoin")
    sparkAction.failureTransition.get.name should be("errorEmail")

    val hiveAction = forkNode.action.asInstanceOf[Fork].transitionPaths.toSet.filter(_.name == "hiveAction").head
    hiveAction.name should be("hiveAction")
    hiveAction.successTransition.get.name should be("mainJoin")
    hiveAction.failureTransition.get.name should be("errorEmail")

    val joinAction = hiveAction.successTransition.get
    joinAction.name should be("mainJoin")
    joinAction.failureTransition should be(None)
    joinAction.successTransition.get.name should be("shellAction")
    joinAction.action.asInstanceOf[Join].transitionTo.name should be("shellAction")

    val shellAction = joinAction.successTransition.get
    shellAction.name should be("shellAction")
    shellAction.successTransition.get.name should be("end")
    shellAction.failureTransition.get.name should be("errorEmail")

    val emailAction = shellAction.failureTransition.get
    emailAction.name should be("errorEmail")
    emailAction.successTransition.get.name should be("kill")
    emailAction.failureTransition.get.name should be("kill")

    val endAction = shellAction.successTransition.get
    endAction.name should be("end")
    endAction.successTransition should be(None)
    endAction.failureTransition should be(None)

    val killAction = emailAction.successTransition.get
    killAction.name should be("kill")
    killAction.successTransition should be(None)
    killAction.failureTransition should be(None)

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

    val decisionNode = startNode.successTransition.get
    decisionNode.name should be("decisionNode")
    decisionNode.action.asInstanceOf[Decision].defaultPath.name should be("sparkAction")
    decisionNode.action.asInstanceOf[Decision].transitionPaths.map(_.name).toSet should be(
      Set("sparkAction", "hiveAction")
    )

    decisionNode.successTransition should be(None)
    decisionNode.failureTransition should be(None)

    val sparkAction =
      decisionNode.action.asInstanceOf[Decision].transitionPaths.toSet.filter(_.name == "sparkAction").head
    sparkAction.name should be("sparkAction")
    sparkAction.successTransition.get.name should be("shellAction")
    sparkAction.failureTransition.get.name should be("errorEmail")

    val hiveAction =
      decisionNode.action.asInstanceOf[Decision].transitionPaths.toSet.filter(_.name == "hiveAction").head
    hiveAction.name should be("hiveAction")
    hiveAction.successTransition.get.name should be("shellAction")
    hiveAction.failureTransition.get.name should be("errorEmail")

    val shellAction = sparkAction.successTransition.get
    shellAction.name should be("shellAction")
    shellAction.successTransition.get.name should be("end")
    shellAction.failureTransition.get.name should be("errorEmail")

    val emailAction = hiveAction.failureTransition.get
    emailAction.name should be("errorEmail")
    emailAction.successTransition.get.name should be("kill")
    emailAction.failureTransition.get.name should be("kill")

    val endAction = shellAction.successTransition.get
    endAction.name should be("end")
    endAction.successTransition should be(None)
    endAction.failureTransition should be(None)

    val killAction = emailAction.successTransition.get
    killAction.name should be("kill")
    killAction.successTransition should be(None)
    killAction.failureTransition should be(None)

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
