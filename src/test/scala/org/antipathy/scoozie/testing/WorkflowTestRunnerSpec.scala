package org.antipathy.scoozie.testing

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.action._
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.control._
import scala.xml
import scala.collection.immutable._
import org.antipathy.scoozie.workflow.Workflow
import org.antipathy.scoozie.exception.TransitionException

class WorkflowTestRunnerSpec extends FlatSpec with Matchers {

  behavior of "WorkflowTestRunner"

  it should "traverse a simple workflow" in {

    implicit val credentialsOption: Option[Credentials] = None

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
                                  config = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(shellAction),
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow)

    workflowTestRunner.traversalPath should be("start -> shellAction -> end")

  }

  it should "traverse a simple workflow with a failing action" in {

    implicit val credentialsOption: Option[Credentials] = None

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
                                  config = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(shellAction),
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow, Seq(shellAction.name))

    workflowTestRunner.traversalPath should be(
      "start -> shellAction -> emailAction -> kill"
    )

  }

  it should "traverse a workflow with a fork" in {

    implicit val credentialsOption: Option[Credentials] = None

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
                                  config = yarnConfig)
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
                                  config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val fork = Fork(name = "mainFork", Seq(sparkAction, hiveAction))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(fork),
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow)

    workflowTestRunner.traversalPath should be(
      "start -> mainFork -> (sparkAction, hiveAction) -> mainJoin -> shellAction -> end"
    )
  }

  it should "traverse a workflow with a fork with a failing action" in {

    implicit val credentialsOption: Option[Credentials] = None

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
                                  config = yarnConfig)
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
                                  config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val fork = Fork(name = "mainFork", Seq(sparkAction, hiveAction))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(fork),
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow, Seq("hiveAction"))

    workflowTestRunner.traversalPath should be(
      "start -> mainFork -> (sparkAction, hiveAction) -> emailAction -> kill"
    )

  }

  it should "traverse a workflow with sequences inside a fork" in {

    implicit val credentialsOption: Option[Credentials] = None

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
                                  config = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val join = Join("mainJoin", shellAction)

    val sparkAction2 = SparkAction(name = "sparkAction2",
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
      .okTo(join)
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
                                  config = yarnConfig)
      .okTo(sparkAction2)
      .errorTo(emailAction)

    val hiveAction2 = HiveAction(name = "hiveAction2",
                                 hiveSettingsXML = "/path/to/settings.xml",
                                 scriptName = "someScript.hql",
                                 scriptLocation = "/path/to/someScript.hql",
                                 parameters = Seq(),
                                 prepareOption = None,
                                 config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                config = yarnConfig)
      .okTo(hiveAction2)
      .errorTo(emailAction)

    val fork = Fork(name = "mainFork", Seq(sparkAction, hiveAction))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(fork),
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow)

    workflowTestRunner.traversalPath should be(
      "start -> " +
      "mainFork -> " +
      "(sparkAction -> sparkAction2, hiveAction -> hiveAction2) -> " +
      "mainJoin -> " +
      "shellAction -> " +
      "end"
    )
  }

  it should "traverse a workflow with fork sequences of varying length" in {

    implicit val credentialsOption: Option[Credentials] = None

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
                                  config = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val join = Join("mainJoin", shellAction)

    val hiveAction2 = HiveAction(name = "hiveAction2",
                                 hiveSettingsXML = "/path/to/settings.xml",
                                 scriptName = "someScript.hql",
                                 scriptLocation = "/path/to/someScript.hql",
                                 parameters = Seq(),
                                 prepareOption = None,
                                 config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val sparkAction2 = SparkAction(name = "sparkAction2",
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
      .okTo(hiveAction2)
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
                                  config = yarnConfig)
      .okTo(sparkAction2)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val fork = Fork(name = "mainFork", Seq(sparkAction, hiveAction))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(fork),
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow)

    workflowTestRunner.traversalPath should be(
      "start -> " +
      "mainFork -> " +
      "(sparkAction -> sparkAction2 -> hiveAction2, hiveAction) -> " +
      "mainJoin -> " +
      "shellAction -> " +
      "end"
    )
  }

  it should "traverse a workflow with fork sequences of varying length and failing actions" in {

    implicit val credentialsOption: Option[Credentials] = None

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
                                  config = yarnConfig)
      .okTo(End())
      .errorTo(emailAction)

    val join = Join("mainJoin", shellAction)

    val sparkAction2 = SparkAction(name = "sparkAction2",
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
      .okTo(join)
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
                                  config = yarnConfig)
      .okTo(sparkAction2)
      .errorTo(emailAction)

    val hiveAction2 = HiveAction(name = "hiveAction2",
                                 hiveSettingsXML = "/path/to/settings.xml",
                                 scriptName = "someScript.hql",
                                 scriptLocation = "/path/to/someScript.hql",
                                 parameters = Seq(),
                                 prepareOption = None,
                                 config = yarnConfig)
      .okTo(join)
      .errorTo(emailAction)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                config = yarnConfig)
      .okTo(hiveAction2)
      .errorTo(emailAction)

    val fork = Fork(name = "mainFork", Seq(sparkAction, hiveAction))

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = Start().okTo(fork),
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner =
      WorkflowTestRunner(workflow, failingNodes = Seq(sparkAction2.name))

    workflowTestRunner.traversalPath should be(
      "start -> " +
      "mainFork -> " +
      "(sparkAction -> sparkAction2, hiveAction -> hiveAction2) -> " +
      "emailAction -> " +
      "kill"
    )
  }

  it should "detect loops in a workflow" in {
    import org.antipathy.scoozie.exception.LoopingException
    implicit val credentialsOption: Option[Credentials] = None

    val yarnConfig =
      YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")

    val kill = Kill("workflow failed")

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
                                  config = yarnConfig)

    val hiveAction2 = HiveAction(name = "hiveAction2",
                                 hiveSettingsXML = "/path/to/settings.xml",
                                 scriptName = "someScript.hql",
                                 scriptLocation = "/path/to/someScript.hql",
                                 parameters = Seq(),
                                 prepareOption = None,
                                 config = yarnConfig)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                config = yarnConfig)

    val transitions = {

      val hiveDuplicate = hiveAction okTo End() errorTo kill
      val spark = sparkAction okTo hiveDuplicate errorTo kill
      val hive2 = hiveAction2 okTo spark errorTo kill
      val hive = hiveAction okTo hive2 errorTo kill

      Start() okTo hive
    }

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = transitions,
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow)

    an[LoopingException] should be thrownBy {
      workflowTestRunner.traversalPath
    }
  }

  it should "traverse decision nodes when one is specified" in {

    implicit val credentialsOption: Option[Credentials] = None

    val yarnConfig =
      YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")

    val kill = Kill("workflow failed")

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
                                  config = yarnConfig)
    val hiveAction3 = HiveAction(name = "hiveAction3",
                                  hiveSettingsXML = "/path/to/settings.xml",
                                  scriptName = "someScript.hql",
                                  scriptLocation = "/path/to/someScript.hql",
                                  parameters = Seq(),
                                  prepareOption = None,
                                  config = yarnConfig)

    val hiveAction2 = HiveAction(name = "hiveAction2",
                                 hiveSettingsXML = "/path/to/settings.xml",
                                 scriptName = "someScript.hql",
                                 scriptLocation = "/path/to/someScript.hql",
                                 parameters = Seq(),
                                 prepareOption = None,
                                 config = yarnConfig)

    val hiveAction = HiveAction(name = "hiveAction",
                                hiveSettingsXML = "/path/to/settings.xml",
                                scriptName = "someScript.hql",
                                scriptLocation = "/path/to/someScript.hql",
                                parameters = Seq(),
                                prepareOption = None,
                                config = yarnConfig)

    val transitions = {
      val spark = sparkAction okTo End() errorTo kill
      val hive3 = hiveAction3 okTo End() errorTo kill
      val hive2 = hiveAction2 okTo End() errorTo kill
      val decision = Decision("decisionNode",spark,Switch(hive2,"${someVar}"),Switch(hive3,"${someOtherVar}"))
      val hive = hiveAction okTo decision errorTo kill
      Start() okTo hive
    }

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "",
                            transitions = transitions,
                            configurationOption = Some(
                              Configuration(
                                Seq(
                                  Property(name = "workflowprop",
                                           value = "workflowpropvalue")
                                )
                              )
                            ),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow, Seq(), Seq(hiveAction3.name))

    workflowTestRunner.traversalPath should be (
      "start -> hiveAction -> decisionNode -> hiveAction3 -> end"
    )
  }

  it should "traverse a defsult path when no decision is specifed" in {
    implicit val credentialsOption: Option[Credentials] = None

    val yarnConfig =
      YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")

    val kill = Kill("workflow failed")

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
      config = yarnConfig)
    val hiveAction3 = HiveAction(name = "hiveAction3",
      hiveSettingsXML = "/path/to/settings.xml",
      scriptName = "someScript.hql",
      scriptLocation = "/path/to/someScript.hql",
      parameters = Seq(),
      prepareOption = None,
      config = yarnConfig)

    val hiveAction2 = HiveAction(name = "hiveAction2",
      hiveSettingsXML = "/path/to/settings.xml",
      scriptName = "someScript.hql",
      scriptLocation = "/path/to/someScript.hql",
      parameters = Seq(),
      prepareOption = None,
      config = yarnConfig)

    val hiveAction = HiveAction(name = "hiveAction",
      hiveSettingsXML = "/path/to/settings.xml",
      scriptName = "someScript.hql",
      scriptLocation = "/path/to/someScript.hql",
      parameters = Seq(),
      prepareOption = None,
      config = yarnConfig)

    val transitions = {
      val spark = sparkAction okTo End() errorTo kill
      val hive3 = hiveAction3 okTo End() errorTo kill
      val hive2 = hiveAction2 okTo End() errorTo kill
      val decision = Decision("decisionNode",spark,Switch(hive2,"${someVar}"),Switch(hive3,"${someOtherVar}"))
      val hive = hiveAction okTo decision errorTo kill
      Start() okTo hive
    }

    val workflow = Workflow(name = "sampleWorkflow",
      path = "",
      transitions = transitions,
      configurationOption = Some(
        Configuration(
          Seq(
            Property(name = "workflowprop",
              value = "workflowpropvalue")
          )
        )
      ),
      yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow, Seq(hiveAction3.name))

    workflowTestRunner.traversalPath should be (
      "start -> hiveAction -> decisionNode -> sparkAction -> end"
    )
  }

  it should "raise an error when multiple decisin paths are specified" in {

    implicit val credentialsOption: Option[Credentials] = None

    val yarnConfig =
      YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")

    val kill = Kill("workflow failed")

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
      config = yarnConfig)
    val hiveAction3 = HiveAction(name = "hiveAction3",
      hiveSettingsXML = "/path/to/settings.xml",
      scriptName = "someScript.hql",
      scriptLocation = "/path/to/someScript.hql",
      parameters = Seq(),
      prepareOption = None,
      config = yarnConfig)

    val hiveAction2 = HiveAction(name = "hiveAction2",
      hiveSettingsXML = "/path/to/settings.xml",
      scriptName = "someScript.hql",
      scriptLocation = "/path/to/someScript.hql",
      parameters = Seq(),
      prepareOption = None,
      config = yarnConfig)

    val hiveAction = HiveAction(name = "hiveAction",
      hiveSettingsXML = "/path/to/settings.xml",
      scriptName = "someScript.hql",
      scriptLocation = "/path/to/someScript.hql",
      parameters = Seq(),
      prepareOption = None,
      config = yarnConfig)

    val transitions = {
      val spark = sparkAction okTo End() errorTo kill
      val hive3 = hiveAction3 okTo End() errorTo kill
      val hive2 = hiveAction2 okTo End() errorTo kill
      val decision = Decision("decisionNode",spark,Switch(hive2,"${someVar}"),Switch(hive3,"${someOtherVar}"))
      val hive = hiveAction okTo decision errorTo kill
      Start() okTo hive
    }

    val workflow = Workflow(name = "sampleWorkflow",
      path = "",
      transitions = transitions,
      configurationOption = Some(
        Configuration(
          Seq(
            Property(name = "workflowprop",
              value = "workflowpropvalue")
          )
        )
      ),
      yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow,Seq(), Seq(hiveAction3.name,sparkAction.name))

    an[TransitionException] should be thrownBy{
      workflowTestRunner.traversalPath
    }
  }
}
