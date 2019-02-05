package org.antipathy.scoozie

import org.antipathy.scoozie.action._
import org.antipathy.scoozie.action.prepare.{Prepare => ActionPrepare}
import org.antipathy.scoozie.action.prepare.PrepareFSAction
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.control._
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.formatter.OozieXmlFormatter
import org.antipathy.scoozie.functions._
import org.antipathy.scoozie.testing.WorkflowTestRunner
import org.antipathy.scoozie.validator.{OozieValidator, SchemaType}
import org.antipathy.scoozie.workflow.Workflow
import scala.collection.immutable._
import org.antipathy.scoozie.action.filesystem._

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
      * DistCP action definition
      * @param name the name of the action
      * @param arguments arguments to the DistCP action
      * @param javaOptions java options to pass to the action
      * @param configuration additional configuration to pass to the action
      * @param yarnConfig the yarn configuration
      * @param prepareOption optional preparation step
      */
    def distCP(name: String,
               configuration: Configuration,
               yarnConfig: YarnConfig,
               prepareOption: Option[ActionPrepare] = None,
               arguments: Seq[String],
               javaOptions: String)(implicit credentialsOption: Option[Credentials]): Node =
      DistCPAction(name, arguments, javaOptions, configuration, yarnConfig, prepareOption)

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
    ): Node =
      EmailAction(name, to, cc, subject, body)

    /**
      * oozie end control node
      */
    def end: Node = End()

    /**
      * Oozie Fork control node
      * @param name the name of the fork
      * @param nodes the nodes within the fork
      */
    def fork(name: String, nodes: Seq[Node]): Node = Fork(name, nodes)

    /**
      * Oozie filesystem action
      * @param name the name of the action
      * @param action the actions to perform
      */
    def fs(name: String, action: FileSystemAction*)(implicit credentialsOption: Option[Credentials]): Node =
      FsAction(name, action)

    /**
      * Oozie Hive action
      * @param name the name of the action
      * @param hiveSettingsXML the path to the hive settings XML
      * @param scriptName the name of the hive script
      * @param scriptLocation the path to the hive script
      * @param parameters a collection of parameters to the hive script
      * @param configuration additional config for this action
      * @param yarnConfig Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def hive(name: String,
             configuration: Configuration,
             yarnConfig: YarnConfig,
             prepareOption: Option[ActionPrepare] = None,
             hiveSettingsXML: String,
             scriptName: String,
             scriptLocation: String,
             parameters: Seq[String])(implicit credentialsOption: Option[Credentials]): Node =
      HiveAction(name,
                 hiveSettingsXML,
                 scriptName,
                 scriptLocation,
                 parameters,
                 configuration,
                 yarnConfig,
                 prepareOption)

    /**
      * Oozie Hive action definition
      * @param name the name of the action
      * @param hiveSettingsXML the path to the hive settings XML
      * @param scriptName the name of the hive script
      * @param scriptLocation the path to the hive script
      * @param parameters a collection of parameters to the hive script
      * @param configuration additional config for this action
      * @param yarnConfig Yarn configuration for this action
      * @param jdbcUrl The JDBC URL for the Hive Server 2
      * @param password Password of the current user (non-kerberos environments)
      * @param prepareOption an optional prepare stage for the action
      */
    def hive2(name: String,
              configuration: Configuration,
              yarnConfig: YarnConfig,
              prepareOption: Option[ActionPrepare] = None,
              hiveSettingsXML: String,
              scriptName: String,
              scriptLocation: String,
              parameters: Seq[String],
              jdbcUrl: String,
              password: Option[String] = None)(implicit credentialsOption: Option[Credentials]): Node =
      Hive2Action(name,
                  hiveSettingsXML,
                  scriptName,
                  scriptLocation,
                  parameters,
                  configuration,
                  yarnConfig,
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
      * @param configuration additional config for this action
      * @param yarnConfig Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def java(name: String,
             configuration: Configuration,
             yarnConfig: YarnConfig,
             prepareOption: Option[ActionPrepare] = None,
             mainClass: String,
             javaJar: String,
             javaOptions: String,
             commandLineArgs: Seq[String],
             files: Seq[String],
             captureOutput: Boolean)(implicit credentialsOption: Option[Credentials]): Node =
      JavaAction(name,
                 mainClass,
                 javaJar,
                 javaOptions,
                 commandLineArgs,
                 files,
                 captureOutput,
                 configuration,
                 yarnConfig,
                 prepareOption)

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
      * Oozie Java action definition
      * @param name the name of the action
      * @param script the location of the pig script
      * @param params arguments to the script
      * @param jobXml optional job.xml for the script
      * @param configuration additional config for this action
      * @param yarnConfig Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def pig(name: String,
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[ActionPrepare] = None,
            script: String,
            params: Seq[String],
            jobXml: Option[String] = None)(implicit credentialsOption: Option[Credentials]): Node =
      PigAction(name, script, params, jobXml, configuration, yarnConfig, prepareOption)

    /**
      *
      * Oozie Hive action
      * @param name the name of the action
      * @param scriptName the name of the script
      * @param scriptLocation the path to the script
      * @param commandLineArgs command line arguments for script
      * @param envVars environment variables for the script
      * @param files files to include with the script
      * @param configuration additional config for this action
      * @param yarnConfig Yarn configuration for this action
      * @param prepareOption an optional prepare stage for the action
      */
    def shell(name: String,
              configuration: Configuration,
              yarnConfig: YarnConfig,
              prepareOption: Option[ActionPrepare] = None,
              scriptName: String,
              scriptLocation: String,
              commandLineArgs: Seq[String],
              envVars: Seq[String],
              files: Seq[String],
              captureOutput: Boolean)(implicit credentialsOption: Option[Credentials]): Node =
      ShellAction(name,
                  scriptName,
                  scriptLocation,
                  commandLineArgs,
                  envVars,
                  files,
                  captureOutput,
                  configuration,
                  yarnConfig,
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
      * @param configuration additional config for this action
      * @param yarnConfig Yarn configuration for this action
      */
    def spark(name: String,
              configuration: Configuration,
              yarnConfig: YarnConfig,
              prepareOption: Option[ActionPrepare] = None,
              sparkSettings: String,
              sparkMasterURL: String,
              sparkMode: String,
              sparkJobName: String,
              mainClass: String,
              sparkJar: String,
              sparkOptions: String,
              commandLineArgs: Seq[String],
              files: Seq[String])(implicit credentialsOption: Option[Credentials]): Node =
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
                  configuration,
                  yarnConfig)

    /**
      * Oozie Sqoop action definition
      * @param name the name of the action
      * @param command the sqoop command
      * @param files files to include with the action
      * @param configuration configuration to provide to the action
      * @param yarnConfig the yarn configuration
      * @param prepareOption an optional prepare step
      */
    def sqoopAction(name: String,
                    configuration: Configuration,
                    yarnConfig: YarnConfig,
                    prepareOption: Option[ActionPrepare] = None,
                    command: String,
                    files: Seq[String])(implicit credentialsOption: Option[Credentials]): Node =
      SqoopAction(name, Some(command), Seq.empty, files, configuration, yarnConfig, prepareOption)

    /**
      * Oozie Sqoop action definition
      * @param name the name of the action
      * @param args arguments to specify to sqoop (ignored if command is specified)
      * @param files files to include with the action
      * @param configuration configuration to provide to the action
      * @param yarnConfig the yarn configuration
      * @param prepareOption an optional prepare step
      */
    def sqoopAction(name: String,
                    configuration: Configuration,
                    yarnConfig: YarnConfig,
                    prepareOption: Option[ActionPrepare] = None,
                    args: Seq[String],
                    files: Seq[String])(implicit credentialsOption: Option[Credentials]): Node =
      SqoopAction(name, None, args, files, configuration, yarnConfig, prepareOption)

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
    def start: Node = Start()

    /**
      * Oozie sub-workflow action definition
      * @param name the name of the action
      * @param applicationPath The path to the workflow
      * @param propagateConfiguration should the parent workflow properties be used
      * @param configuration configuration to provide to the action
      * @param yarnConfig the yarn configuration
      */
    def subWorkflow(name: String,
                    configuration: Configuration,
                    yarnConfig: YarnConfig,
                    applicationPath: String,
                    propagateConfiguration: Boolean,
    )(implicit credentialsOption: Option[Credentials]): Node =
      SubWorkflowAction(name, applicationPath, propagateConfiguration, configuration, yarnConfig)

    /**
      * Ooozie decision node switch
      *
      * @param node the node to switch to
      * @param predicate the predicate for switching to the node
      */
    def switch(node: Node, predicate: String): Switch = Switch(node, predicate)
  }

  /**
    * Oozie filesystem operations
    */
  object FileSystem {

    /**
      * Create an ozzie chmod step
      *
      * @param path the path to operate on
      * @param permissions the permissions to set
      * @param dirFiles should the operation be recursive
      */
    def chmod(path: String, permissions: String, dirFiles: String): Chmod = Chmod(path, permissions, dirFiles)

    /**
      * Create a delete step
      *
      * @param path the path to delete
      * @return a delete step
      */
    def delete(path: String): Delete = Delete(path)

    /**
      * Create a make directory step
      * @param path the path to create
      * @return a make directory step
      */
    def makeDirectory(path: String): MakeDir = MakeDir(path)

    /**
      * Create an ozzie move step
      * @param srcPath the path to move from
      * @param targetPath the path to move to
      */
    def move(srcPath: String, targetPath: String): Move = Move(srcPath, targetPath)

    /**
      * Create a touch step
      * @param path the path to touch
      * @return a touch step
      */
    def touchz(path: String): Touchz = Touchz(path)
  }

  /**
    * Action preparation methods
    */
  object Prepare {

    /**
      * Create a delete preparation step
      *
      * @param path the path to delete
      * @return a delete preparation step
      */
    def delete(path: String): Delete = Delete(path)

    /**
      * Create a make directory preparation step
      * @param path the path to create
      * @return a make directory preparation step
      */
    def makeDirectory(path: String): MakeDir = MakeDir(path)

    /**
      * Create an action preparation step
      * @param actions the preparation actions
      * @return an action preparation step
      */
    def prepare(actions: Seq[PrepareFSAction]): Option[ActionPrepare] =
      Some(ActionPrepare(actions))
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
      * Empty Oozie configuration for a workflow or an action
      */
    def emptyConfiguration: Configuration = Configuration(Seq.empty)

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
    def yarnConfiguration(jobTracker: String, nameNode: String, configuration: Configuration = emptyConfiguration) =
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
    def validate(coOrdinator: Coordinator): Unit =
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
    * Oozie workflow
    *
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
               yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]): Workflow =
    Workflow(name, path, transitions, configurationOption, yarnConfig)

  def coordinator(name: String,
                  frequency: String,
                  start: String,
                  end: String,
                  timezone: String,
                  workflow: Workflow,
                  configurationOption: Option[Configuration] = None): Coordinator =
    Coordinator(name, frequency, start, end, timezone, workflow, configurationOption)

  object Functions {

    /**
      * Oozie workflow functions
      */
    object WorkFlow {

      /**
        * returns the workflow job ID for the current workflow job.
        */
      val id: String = WorkflowFunctions.id

      /**
        * returns the workflow application name for the current workflow job.
        */
      val name: String = WorkflowFunctions.name

      /**
        * returns the workflow application path for the current workflow job.
        */
      val appPath: String = WorkflowFunctions.appPath

      /**
        * returns the value of the workflow job configuration property for the current workflow job,
        * or an empty string if undefined.
        */
      def conf(name: String): String = WorkflowFunctions.conf(name)

      /**
        * returns the user name that started the current workflow job.
        */
      val user: String = WorkflowFunctions.user

      /**
        * returns the group/ACL for the current workflow job
        */
      val group: String = WorkflowFunctions.group

      /**
        * returns the callback URL for the current workflow action node, stateVar can be a valid exit
        * state (=OK= or ERROR ) for the action or a token to be replaced with the exit state by the remote
        * system executing the task.
        */
      def callBack(stateVar: String): String = WorkflowFunctions.callBack(stateVar)

      /**
        * returns the transition taken by the specified workflow action node, or an empty
        * string if the action has not being executed or it has not completed yet.
        */
      def transition(nodeName: String): String = WorkflowFunctions.transition(nodeName)

      /**
        * returns the name of the last workflow action node that exit with an ERROR exit state, or an empty string
        * if no a ction has exited with ERROR state in the current workflow job.
        */
      val lastErrorNode: String = WorkflowFunctions.lastErrorNode

      /**
        * returns the error code for the specified action node, or an empty string
        * if the action node has not exited with ERROR state.
        * Each type of action node must define its complete error code list.
        */
      def errorCode(nodeName: String): String = WorkflowFunctions.errorCode(nodeName)

      /**
        * returns the error message for the specified action node, or an empty string if no
        * action node has not exited with ERROR state.
        * The error message can be useful for debugging and notification purposes.
        */
      def errorMessage(nodeName: String): String = WorkflowFunctions.errorMessage(nodeName)

      /**
        * returns the run number for the current workflow job, normally 0 unless the workflow
        * job is re-run, in which case indicates the current run.
        */
      val run: String = WorkflowFunctions.run

      /**
        * This function is only applicable to action nodes that produce output data on completion.
        * The output data is in a Java Properties format and via this EL function it is available as a Map .
        */
      def actionData(nodeName: String): String = WorkflowFunctions.actionData(nodeName)

      /**
        * returns the external Id for an action node, or an empty string if the action has
        * not being executed or it has not completed yet.
        */
      def externalActionId(nodeName: String): String = WorkflowFunctions.externalActionId(nodeName)

      /**
        * returns the tracker URIfor an action node, or an empty string if the action has
        * not being executed or it has not completed yet.
        */
      def actionTrackerURL(nodeName: String): String = WorkflowFunctions.actionTrackerURL(nodeName)

      /**
        * returns the external status for an action node, or an empty string if the action has not
        * being executed or it has not completed yet.
        */
      def actionExternalStatus(nodeName: String): String = WorkflowFunctions.actionExternalStatus(nodeName)
    }

    /**
      * Oozie basic functions
      */
    object Basic {

      /**
        * returns the first not null value, or null if both are null .Note that if the output of this function is null and
        * it is used as string, the EL library converts it to an empty string. This is the common behavior when using
        * firstNotNull() in node configuration sections.
        */
      def firstNotNull(value1: String, value2: String): String = BasicFunctions.firstNotNull(value1, value2)

      /**
        * returns the concatenation of 2 strings. A string with null value is considered as an empty string.
        */
      def concat(s1: String, s2: String): String = BasicFunctions.concat(s1, s2)

      /**
        * Replace each occurrence of regular expression match in the first string with the replacement
        * string and return the replaced string. A 'regex' string with null value is considered as no change.
        * A 'replacement' string with null value is consider as an empty string.
        */
      def replaceAll(src: String, regex: String, replacement: String): String =
        BasicFunctions.replaceAll(src, regex, replacement)

      /**
        * Add the append string into each splitted sub-strings of the first string(=src=). The split is performed
        * into src string using the delimiter . E.g. appendAll("/a/b/,/c/b/,/c/d/", "ADD", ",") will
        * return /a/b/ADD,/c/b/ADD,/c/d/ADD . A append string with null value is consider as an empty string.
        * A delimiter string with value null is considered as no append in the string.
        */
      def appendAll(src: String, append: String, delimeter: String): String =
        BasicFunctions.appendAll(src, append, delimeter)

      /**
        * returns the trimmed value of the given string. A string with null value is considered as an empty string.
        */
      def trim(s: String): String = BasicFunctions.trim(s)

      /**
        * returns the URL UTF-8 encoded value of the given string. A string with null
        * value is considered as an empty string.
        */
      def urlEncode(s: String): String = BasicFunctions.urlEncode(s)

      /**
        * returns the UTC current date and time in W3C format down to the
        * second (YYYY-MM-DDThh:mm:ss.sZ). I.e.: 1997-07-16T19:20:30.45Z
        */
      val timestamp: String = BasicFunctions.timestamp

      /**
        * returns an XML encoded JSON representation of a Map. This function is useful to encode as a single
        * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it
        * in full to another action.
        */
      def toJsonStr(variable: String): String = BasicFunctions.toJsonStr(variable)

      /**
        * returns an XML encoded Properties representation of a Map. This function is useful to encode as a single
        * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it in
        * full to another action.
        */
      def toPropertiesStr(variable: String): String = BasicFunctions.toPropertiesStr(variable)

      /**
        * returns an XML encoded Configuration representation of a Map. This function is useful to encode as a single
        * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it in full
        * to another action.
        */
      def toConfigurationStr(variable: String): String = BasicFunctions.toConfigurationStr(variable)
    }

    /**
      * oozie coordinator functions
      */
    object Coordinator {

      /**
        * Oozie time frequency in minutes
        */
      def minutes(i: Int): String = CoordinatorFunctions.minutes(i)

      /**
        * Oozie time frequency in hours
        */
      def hours(i: Int): String = CoordinatorFunctions.hours(i)

      /**
        * Oozie time frequency in days
        */
      def days(i: Int): String = CoordinatorFunctions.days(i)

      /**
        * Oozie time frequency in days
        * identical to the `days` function except that it shifts the first occurrence to the end of the day
        * for the specified timezone before computing the interval in minutes
        */
      def endOfDays(i: Int): String = CoordinatorFunctions.endOfDays(i)

      /**
        * Oozie time frequency in months
        */
      def months(i: Int): String = CoordinatorFunctions.months(i)

      /**
        * Oozie time frequency in months
        * identical to the `months` function except that it shifts the first occurrence to the end of the month
        * for the specified timezone before computing the interval in minutes
        */
      def endOfMonths(i: Int): String = CoordinatorFunctions.endOfMonths(i)

      /**
        * Oozie time frequency in cron format
        */
      def cron(string: String): String = CoordinatorFunctions.cron(string)
    }
  }
}
