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
package org.antipathy.scoozie.traits

import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.workflow.Workflow

import scala.collection.immutable.{Map, Seq}

/**
  * Test class for interface
  */
class TestCoordinator(jobTracker: String, nameNode: String, yarnProperties: Map[String, String])
    extends ScoozieWorkflow
    with ScoozieCoordinator {
  import org.antipathy.scoozie.Scoozie

  private implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials
  private val yarnConfig = Scoozie.Configuration.yarnConfig(jobTracker, nameNode)
  private val kill = Scoozie.Actions.kill("Workflow failed")

  private val sparkSLA = Scoozie.SLA.create(nominalTime = "nominal_time",
                                            shouldStart = Some("10 * MINUTES"),
                                            shouldEnd = Some("30 * MINUTES"),
                                            maxDuration = Some("30 * MINUTES"),
                                            alertEvents = Scoozie.SLA.Alerts.all,
                                            alertContacts = Seq("some@one.com"))

  private val sparkAction = Scoozie.Actions
    .spark(name = "doASparkThing",
           jobXmlOption = Some("/path/to/job/xml"),
           sparkMasterURL = "masterURL",
           sparkMode = "mode",
           sparkJobName = "JobName",
           mainClass = "org.antipathy.Main",
           sparkJar = "/path/to/jar",
           sparkOptions = "spark options",
           commandLineArgs = Seq(),
           prepareOption = None,
           configuration = Scoozie.Configuration.emptyConfig,
           yarnConfig = yarnConfig)
    .withSLA(sparkSLA)

  private val emailAction = Scoozie.Actions.email(name = "alertFailure",
                                                  to = Seq("a@a.com", "b@b.com"),
                                                  cc = Seq.empty,
                                                  subject = "message subject",
                                                  body = "message body")

  private val shellAction = Scoozie.Actions.shell(name = "doAShellThing",
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

  private val hiveAction = Scoozie.Actions.hive(name = "doAHiveThing",
                                                scriptName = "someScript.hql",
                                                scriptLocation = "/path/to/someScript.hql",
                                                parameters = Seq(),
                                                jobXmlOption = Some("/path/to/settings.xml"),
                                                files = Seq(),
                                                prepareOption = None,
                                                configuration = Scoozie.Configuration.emptyConfig,
                                                yarnConfig = yarnConfig)

  private val javaAction = Scoozie.Actions.java(name = "doAJavaThing",
                                                mainClass = "org.antipathy.Main",
                                                javaJar = "/path/to/jar",
                                                javaOptions = "java options",
                                                commandLineArgs = Seq(),
                                                captureOutput = false,
                                                jobXmlOption = None,
                                                files = Seq(),
                                                prepareOption =
                                                  Scoozie.Prepare.prepare(Seq(Scoozie.Prepare.delete("/some/path"))),
                                                configuration = Scoozie.Configuration.emptyConfig,
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
                                                     jobXmlOption = None,
                                                     configuration = Scoozie.Configuration.emptyConfig,
                                                     yarnConfig = yarnConfig)

  override val coordinator: Coordinator = Scoozie.coordinator(name = "ExampleCoOrdinator",
                                                              path = "somePath",
                                                              frequency = "startFreq",
                                                              start = "start",
                                                              end = "end",
                                                              timezone = "timeZome",
                                                              workflow = workflow,
                                                              configuration =
                                                                Scoozie.Configuration.configuration(yarnProperties))
}
