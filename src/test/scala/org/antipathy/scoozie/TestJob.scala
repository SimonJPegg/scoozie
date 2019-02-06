package org.antipathy.scoozie

import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.workflow.Workflow
import scala.collection.immutable.{Map, Seq}

/**
  * Test class for interface
  */
class TestJob(jobTracker: String, nameNode: String, yarnProperties: Map[String, String])
    extends GeneratedWorkflow
    with GeneratedCoordinator {

  private implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials
  private val yarnConfig = Scoozie.Configuration.yarnConfiguration(jobTracker, nameNode)
  private val kill = Scoozie.Actions.kill("Workflow failed")

  private val sparkAction = Scoozie.Actions.spark(name = "doASparkThing",
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
                                                  configuration = Scoozie.Configuration.emptyConfiguration,
                                                  yarnConfig = yarnConfig)

  private val emailAction = Scoozie.Actions.email(name = "alertFailure",
                                                  to = Seq("a@a.com", "b@b.com"),
                                                  subject = "message subject",
                                                  body = "message body")

  private val shellAction = Scoozie.Actions.shell(name = "doAShellThing",
                                                  prepareOption = None,
                                                  scriptName = "script.sh",
                                                  scriptLocation = "/path/to/script.sh",
                                                  commandLineArgs = Seq(),
                                                  envVars = Seq(),
                                                  files = Seq(),
                                                  captureOutput = true,
                                                  configuration = Scoozie.Configuration.emptyConfiguration,
                                                  yarnConfig = yarnConfig)

  private val hiveAction = Scoozie.Actions.hive(name = "doAHiveThing",
                                                hiveSettingsXML = "/path/to/settings.xml",
                                                scriptName = "someScript.hql",
                                                scriptLocation = "/path/to/someScript.hql",
                                                parameters = Seq(),
                                                prepareOption = None,
                                                configuration = Scoozie.Configuration.emptyConfiguration,
                                                yarnConfig = yarnConfig)

  private val javaAction = Scoozie.Actions.java(name = "doAJavaThing",
                                                mainClass = "org.antipathy.Main",
                                                javaJar = "/path/to/jar",
                                                javaOptions = "java options",
                                                commandLineArgs = Seq(),
                                                captureOutput = false,
                                                files = Seq(),
                                                prepareOption =
                                                  Scoozie.Prepare.prepare(Seq(Scoozie.Prepare.delete("/some/path"))),
                                                configuration = Scoozie.Configuration.emptyConfiguration,
                                                yarnConfig = yarnConfig)

  private val start = Scoozie.Actions.start

  private val transitions = {
    val errorMail = emailAction okTo kill errorTo kill
    val mainJoin = Scoozie.Actions.join("mainJoin", Scoozie.Actions.end)
    val java = javaAction okTo mainJoin errorTo errorMail
    val hive = hiveAction okTo mainJoin errorTo errorMail
    val mainFork = Scoozie.Actions.fork("mainFork", Seq(java, hive))
    val shell = shellAction okTo mainFork errorTo errorMail
    val spark = sparkAction okTo mainFork errorTo errorMail
    val decision = Scoozie.Actions.decision("sparkOrShell", spark, Scoozie.Actions.switch(shell, "${someVar}"))
    start okTo decision
  }

  override val workflow: Workflow = Scoozie.workflow(name = "ExampleWorkflow",
                                                     path = "/path/to/workflow.xml",
                                                     transitions = transitions,
                                                     configuration = Scoozie.Configuration.emptyConfiguration,
                                                     yarnConfig = yarnConfig)

  override val coordinator: Coordinator = Scoozie.coordinator(name = "ExampleCoOrdinator",
                                                              frequency = "startFreq",
                                                              start = "start",
                                                              end = "end",
                                                              timezone = "timeZome",
                                                              workflow = workflow,
                                                              configuration =
                                                                Scoozie.Configuration.configuration(yarnProperties))
}
