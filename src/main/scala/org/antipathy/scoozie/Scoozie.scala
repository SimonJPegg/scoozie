package org.antipathy.scoozie

import org.antipathy.scoozie.action._
import org.antipathy.scoozie.action.prepare._
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.control._
import org.antipathy.scoozie.coordinator.CoOrdinator
import org.antipathy.scoozie.formatter.OozieXmlFormatter
import org.antipathy.scoozie.testing.WorkflowTestRunner
import org.antipathy.scoozie.validator.{OozieValidator, SchemaType}
import org.antipathy.scoozie.workflow.Workflow
import scala.collection.immutable._

/**
  * Entry class for Scoozie.
  *
  * All required functionality is exposed through this object, however you are not limited to accessing
  * the library via this interface
  */
object Scoozie {

  /**
    * Oozie workflow actions
    */
  object Action {

    /**
      * Oozie decision control node
      * @param name the name of the decision node
      * @param switch the switches in the decision node
      * @param default the default action for the decision
      */
    def decision(name: String, default: Node, switch: Switch*): Node =
      Decision(name, default, Seq(switch.toSeq: _*))

    /**
      * Email action
      * @param name the name of the action
      * @param to the to recipient list
      * @param cc an optional cc recipient list
      * @param subject the message subject
      * @param body the message body
      */
    def email(name: String, to: Seq[String], cc: Seq[String] = Seq.empty[String], subject: String, body: String)(
        implicit credentialsOption: Option[Credentials]
    ) =
      EmailAction(name, to, cc, subject, body)

    /**
      * oozie end control node
      */
    def end = End()

    /**
      * Oozie Fork control node
      * @param name the name of the fork
      * @param nodes the nodes within the fork
      */
    def fork(name: String, nodes: Seq[Node]): Node = Fork(name, nodes)

    /**
      * Oozie Hive action
      * @param name the name of the action
      * @param hiveSettingsXML the path to the hive settings XML
      * @param scriptName the name of the hive script
      * @param scriptLocation the path to the hive script
      * @param parameters a collection of parameters to the hive script
      * @param config Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def hive(name: String,
             hiveSettingsXML: String,
             scriptName: String,
             scriptLocation: String,
             parameters: Seq[String],
             config: YarnConfig,
             prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
      HiveAction(name, hiveSettingsXML, scriptName, scriptLocation, parameters, config, prepareOption)

    /**
      * Oozie Hive action definition
      * @param name the name of the action
      * @param hiveSettingsXML the path to the hive settings XML
      * @param scriptName the name of the hive script
      * @param scriptLocation the path to the hive script
      * @param parameters a collection of parameters to the hive script
      * @param config Yarn configuration for this action
      * @param jdbcUrl The JDBC URL for the Hive Server 2
      * @param password Password of the current user (non-kerberos environments)
      * @param prepareOption an optional prepare stage for the action
      */
    def hive2(name: String,
              hiveSettingsXML: String,
              scriptName: String,
              scriptLocation: String,
              parameters: Seq[String],
              config: YarnConfig,
              jdbcUrl: String,
              password: Option[String] = None,
              prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]) =
      Hive2Action(name,
                  hiveSettingsXML,
                  scriptName,
                  scriptLocation,
                  parameters,
                  config,
                  jdbcUrl,
                  password,
                  prepareOption)

    /**
      * Oozie Java action definition
      * @param name the name of the action
      * @param mainClass the main class of the java job
      * @param javaJar the location of the java jar
      * @param javaOptions options for the java job
      * @param commandLineArgs command line arguments for the java job
      * @param files files to include with the application
      * @param captureOutput capture output from this action
      * @param config Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def java(name: String,
             mainClass: String,
             javaJar: String,
             javaOptions: String,
             commandLineArgs: Seq[String],
             files: Seq[String],
             captureOutput: Boolean,
             config: YarnConfig,
             prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
      JavaAction(name, mainClass, javaJar, javaOptions, commandLineArgs, files, captureOutput, config, prepareOption)

    /**
      * Oozie join control node
      * @param name the name of the join
      * @param to the node the join transitions to
      */
    def join(name: String, to: Node): Node = Join(name, to)

    /**
      * Oozie kill control node
      */
    def kill(message: String): Node = Kill(message)

    /**
      * Oozie Java action
      * @param name the name of the action
      * @param script the location of the pig script
      * @param params arguments to the script
      * @param jobXml optional job.xml for the script
      * @param config Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def pig(name: String,
            script: String,
            params: Seq[String],
            jobXml: Option[String] = None,
            config: YarnConfig,
            prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
      PigAction(name, script, params, jobXml, config, prepareOption)

    /**
      *
      * Oozie Hive action
      * @param name the name of the action
      * @param scriptName the name of the script
      * @param scriptLocation the path to the script
      * @param commandLineArgs command line arguments for script
      * @param envVars environment variables for the script
      * @param files files to include with the script
      * @param captureOutput capture output from this action
      * @param config Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def shell(name: String,
              scriptName: String,
              scriptLocation: String,
              commandLineArgs: Seq[String],
              envVars: Seq[String],
              files: Seq[String],
              captureOutput: Boolean,
              config: YarnConfig,
              prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
      ShellAction(name,
                  scriptName,
                  scriptLocation,
                  commandLineArgs,
                  envVars,
                  files,
                  captureOutput,
                  config,
                  prepareOption)

    /**
      * Oozie Spark action
      * @param name the name of the action
      * @param sparkSettings the spark settings location
      * @param sparkMasterURL the url of the spark master
      * @param sparkMode the mode the spark job should run in
      * @param sparkJobName the name of the spark job
      * @param mainClass the main class of the spark job
      * @param sparkJar the location of the spark jar
      * @param sparkOptions options for the spark job
      * @param commandLineArgs command line arguments for the spark job
      * @param prepareOption an optional prepare phase for the action
      * @param config Yarn configuration for this action
      */
    def spark(name: String,
              sparkSettings: String,
              sparkMasterURL: String,
              sparkMode: String,
              sparkJobName: String,
              mainClass: String,
              sparkJar: String,
              sparkOptions: String,
              commandLineArgs: Seq[String],
              files: Seq[String],
              prepareOption: Option[Prepare] = None,
              config: YarnConfig)(implicit credentialsOption: Option[Credentials]): Node =
      SparkAction(name,
                  sparkSettings,
                  sparkMasterURL,
                  sparkMode,
                  sparkJobName,
                  mainClass,
                  sparkJar,
                  sparkOptions,
                  commandLineArgs,
                  files,
                  prepareOption,
                  config)

    /**
      * Oozie SSH action
      *
      * @param name The name of the action
      * @param host The hos to connect to (user@host)
      * @param command The shell command to execute
      * @param args Parameters to be passed to the shell command
      * @param captureOutput Capture output of the STDOUT of the ssh command execution
      */
    def ssh(name: String, host: String, command: String, args: Seq[String], captureOutput: Boolean)(
        implicit credentialsOption: Option[Credentials]
    ): Node =
      SshAction(name, host, command, args, captureOutput)

    /**
      * oozie Start control node
      */
    def start = Start()

    /**
      * Oozie sub-workflow action definition
      * @param name the name of the action
      * @param applicationPath The path to the workflow
      * @param propagateConfiguration should the parent workflow properties be used
      * @param config Yarn config
      */
    def subWorkflow(name: String, applicationPath: String, propagateConfiguration: Boolean, config: YarnConfig)(
        implicit credentialsOption: Option[Credentials]
    ): Node =
      SubWorkflowAction(name, applicationPath, propagateConfiguration, config)

    /**
      * Ooozie decision node switch
      *
      * @param node the node to switch to
      * @param predicate the predicate for switching to the node
      */
    def switch(node: Node, predicate: String) = Switch(node, predicate)
  }

  /**
    * Action preparation methods
    */
  object Prep {

    /**
      * Create a delete preparation step
      * @param path the path to delete
      * @return a delete preparation step
      */
    def delete(path: String) = Delete(path)

    /**
      * Create a make directory preparation step
      * @param path the path to create
      * @return a make directory preparation step
      */
    def makeDirectory(path: String) = MakeDir(path)

    /**
      * Create an action preparation step
      * @param actions the preparation actions
      * @return an action preparation step
      */
    def prepare(actions: Seq[PrepareFSAction]): Option[Prepare] =
      Some(Prepare(actions))
  }

  /**
    * Methods for creating Oozie properties
    */
  object Config {

    /**
      * Create an oozie property
      * @param name the name of the property
      * @param value the value of the property
      * @return an Oozie property
      */
    def property(name: String, value: String): Property = Property(name, value)

    /**
      * Oozie configuration for a workflow or an action
      * @param properties the properties of the configuration
      * @return an oozie configuration
      */
    def configuration(properties: Seq[Property]): Configuration =
      Configuration(properties)

    /**
      * Oozie configuration for a workflow or an action
      * @param properties a map of oozie properties
      * @return
      */
    def configuration(properties: Map[String, String]): Configuration =
      Configuration(Seq(properties.map {
        case (key, value) => Property(key, value)
      }.toSeq: _*))

    /**
      * Create the credentials for an oozie workflow
      * @param name the name of the credential
      * @param credentialsType the type of the credential
      * @param properties the credential's properties
      * @return
      */
    def credentials(name: String, credentialsType: String, properties: Seq[Property]): Option[Credentials] =
      Some(Credentials(Credential(name, credentialsType, properties)))

    /**
      * Create a set of empty credentials for an oozie workflow
      * @return
      */
    def emptyCredentials: Option[Credentials] = None

    /**
      * Create a yarn configuration for an oozie workflow
      *
      * @param jobTracker The oozie job tracker
      * @param nameNode The HDFS name node
      * @param configuration additional yarn configuration options
      * @return a yarn configuration
      */
    def yarnConfiguration(jobTracker: String, nameNode: String, configuration: Configuration = Configuration(Seq())) =
      YarnConfig(jobTracker, nameNode, configuration)

    /**
      * Create a yarn configuration for an oozie workflow
      *
      * @param jobTracker The oozie job tracker
      * @param nameNode The HDFS name node
      * @param config additional yarn configuration options
      * @return a yarn configuration
      */
    def yarnConfiguration(jobTracker: String, nameNode: String, config: Map[String, String]): YarnConfig =
      YarnConfig(jobTracker, nameNode, configuration(config))
  }

  /**
    * Methods for testing Oozie workflows
    */
  object Test {

    /**
      * Wrap the passed in workflow in a test runner
      * @param workflow the workflow to test
      * @param failingNodes a list of nodes that should fail in this workflow
      * @param decisionNodes the nodes to visit on a decision
      */
    def workflowTesterWorkflowTestRunner(workflow: Workflow,
                                         failingNodes: Seq[String] = Seq.empty[String],
                                         decisionNodes: Seq[String] = Seq.empty[String]): WorkflowTestRunner = {
      validate(workflow)
      new WorkflowTestRunner(workflow, failingNodes, decisionNodes)
    }

    /**
      * Validate the passed in workflow
      * @param workflow the workflow to validate
      */
    def validate(workflow: Workflow): Unit =
      OozieValidator.validate(Format.format(workflow, 80, 4), SchemaType.workflow)

    /**
      * Validate the passed in coordinator
      * @param coOrdinator the coordinator to validate
      */
    def validate(coOrdinator: CoOrdinator): Unit =
      OozieValidator.validate(Format.format(coOrdinator, 80, 4), SchemaType.coOrdinator)
  }

  /**
    * Methods for formatting Oozie workflows
    */
  private[scoozie] object Format {

    /**
      * Method for formatting XML nodes
      *
      * @param oozieNode the node to format
      * @param width maximum width of any row
      * @param step indentation for each level of the XML
      * @return XML document in string format
      */
    def format(oozieNode: XmlSerializable, width: Int, step: Int): String =
      new OozieXmlFormatter(width, step).format(oozieNode)
  }

  /**
    * Oozie workflow definition
    * @param name the name of the workflow
    * @param path The path to this workflow
    * @param transitions the actions within the workflow
    * @param configurationOption optional configuration for this workflow
    * @param yarnConfig The yarn configuration for this workflow
    * @param credentialsOption optional credentials for this workflow
    */
  def workflow(name: String,
               path: String,
               transitions: Node,
               configurationOption: Option[Configuration] = None,
               yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]) =
    Workflow(name, path, transitions, configurationOption, yarnConfig)

  /**
    * Oozie coOrdinator definition
    * @param name the CoOrdinator name
    * @param frequency the CoOrdinator frequency
    * @param start the CoOrdinator start time
    * @param end the CoOrdinator end time
    * @param timezone the CoOrdinator time-zone
    * @param workflow the workflow to run
    * @param configuration configuration for the workflow
    */
  def coOrdinator(name: String,
                  frequency: String,
                  start: String,
                  end: String,
                  timezone: String,
                  workflow: Workflow,
                  configuration: Configuration = Configuration(Seq.empty)): CoOrdinator =
    CoOrdinator(name: String,
                frequency: String,
                start: String,
                end: String,
                timezone: String,
                workflow: Workflow,
                configuration: Configuration)
}
