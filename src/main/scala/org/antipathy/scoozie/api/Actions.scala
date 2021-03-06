// $COVERAGE-OFF$
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
package org.antipathy.scoozie.api

import org.antipathy.scoozie.action._
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.action.filesystem.FileSystemAction
import org.antipathy.scoozie.action.prepare.{Prepare => ActionPrepare}
import org.antipathy.scoozie.configuration._

import scala.collection.immutable.Seq

/**
  * Oozie workflow actions
  */
object Actions {

  /**
    * Oozie decision control node
    *
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
    * @param contentTypeOption optional string defining the content type of the message
    */
  def email(name: String,
            to: Seq[String],
            cc: Seq[String],
            subject: String,
            body: String,
            contentTypeOption: Option[String] = None): Node =
    EmailAction(name, to, cc, subject, body, contentTypeOption)

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
    * @param jobXmlOption optional job.xml path
    * @param step the steps to perform
    * @param configuration additional config for this action
    */
  def fs(name: String, jobXmlOption: Option[String], configuration: Configuration, step: FileSystemAction*): Node =
    FsAction(name, Seq(step: _*), jobXmlOption, configuration)

  /**
    * Oozie Hive action
    * @param name the name of the action
    * @param scriptName the name of the hive script
    * @param scriptLocation the path to the hive script
    * @param parameters a collection of parameters to the hive script
    * @param jobXmlOption optional job.xml path
    * @param files additional files to pass to job
    * @param configuration additional config for this action
    * @param yarnConfig Yarn configuration for this action
    * @param prepareOption an optional prepare stage for the action
    */
  def hive(name: String,
           scriptName: String,
           scriptLocation: String,
           parameters: Seq[String],
           jobXmlOption: Option[String],
           files: Seq[String],
           configuration: Configuration,
           yarnConfig: YarnConfig,
           prepareOption: Option[ActionPrepare])(implicit credentialsOption: Option[Credentials]): Node =
    HiveAction(name,
               scriptName,
               scriptLocation,
               parameters,
               jobXmlOption,
               files,
               configuration,
               yarnConfig,
               prepareOption)

  /**
    * Oozie Java action
    * @param name the name of the action
    * @param mainClass the main class of the java job
    * @param javaJar the location of the java jar
    * @param javaOptions options for the java job
    * @param commandLineArgs command line arguments for the java job
    * @param files files to include with the application
    * @param captureOutput capture output from this action
    * @param jobXmlOption optional job.xml path
    * @param configuration additional config for this action
    * @param yarnConfig Yarn configuration for this action
    * @param prepareOption an optional prepare stage for the action
    */
  def java(name: String,
           mainClass: String,
           javaJar: String,
           javaOptions: String,
           commandLineArgs: Seq[String],
           files: Seq[String],
           captureOutput: Boolean,
           jobXmlOption: Option[String],
           configuration: Configuration,
           yarnConfig: YarnConfig,
           prepareOption: Option[ActionPrepare])(implicit credentialsOption: Option[Credentials]): Node =
    JavaAction(name,
               mainClass,
               javaJar,
               javaOptions,
               commandLineArgs,
               files,
               captureOutput,
               jobXmlOption,
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
    * @param params parameters to the script
    * @param arguments arguments to the script
    * @param files additional files to bundle with the job
    * @param jobXmlOption optional job.xml for the script
    * @param configuration additional config for this action
    * @param yarnConfig Yarn configuration for this action
    * @param prepareOption an optional prepare stage for the action
    */
  def pig(name: String,
          script: String,
          params: Seq[String],
          arguments: Seq[String],
          files: Seq[String],
          jobXmlOption: Option[String],
          configuration: Configuration,
          yarnConfig: YarnConfig,
          prepareOption: Option[ActionPrepare])(implicit credentialsOption: Option[Credentials]): Node =
    PigAction(name, script, params, arguments, files, jobXmlOption, configuration, yarnConfig, prepareOption)

  /**
    *
    * Oozie Hive action definition
    * @param name the name of the action
    * @param scriptName the name of the script
    * @param scriptLocation the path to the script
    * @param commandLineArgs command line arguments for script
    * @param envVars environment variables for the script
    * @param files files to include with the script
    * @param captureOutput capture output from this action
    * @param jobXmlOption optional job.xml path
    * @param configuration additional config for this action
    * @param yarnConfig Yarn configuration for this action
    * @param prepareOption an optional prepare stage for the action
    */
  def shell(name: String,
            scriptName: String,
            scriptLocation: String,
            commandLineArgs: Seq[String],
            envVars: Seq[String],
            files: Seq[String],
            captureOutput: Boolean,
            jobXmlOption: Option[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[ActionPrepare])(implicit credentialsOption: Option[Credentials]): Node =
    ShellAction(name,
                scriptName,
                scriptLocation,
                commandLineArgs,
                envVars,
                files,
                captureOutput,
                jobXmlOption,
                configuration,
                yarnConfig,
                prepareOption)

  /**
    * Oozie Spark action definition
    * @param name the name of the action
    * @param sparkMasterURL the url of the spark master
    * @param sparkMode the mode the spark job should run in
    * @param sparkJobName the name of the spark job
    * @param mainClass the main class of the spark job
    * @param sparkJar the location of the spark jar
    * @param sparkOptions options for the spark job
    * @param commandLineArgs command line arguments for the spark job
    * @param jobXmlOption optional job.xml path
    * @param prepareOption an optional prepare phase for the action
    * @param configuration additional config for this action
    * @param yarnConfig Yarn configuration for this action
    */
  def spark(name: String,
            sparkMasterURL: String,
            sparkMode: String,
            sparkJobName: String,
            mainClass: String,
            sparkJar: String,
            sparkOptions: String,
            commandLineArgs: Seq[String],
            jobXmlOption: Option[String],
            prepareOption: Option[ActionPrepare],
            configuration: Configuration,
            yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]): Node =
    SparkAction(name,
                sparkMasterURL,
                sparkMode,
                sparkJobName,
                mainClass,
                sparkJar,
                sparkOptions,
                commandLineArgs,
                jobXmlOption,
                prepareOption,
                configuration,
                yarnConfig)

  /**
    * Oozie Sqoop action definition
    * @param name the name of the action
    * @param command an optional sqoop command (default)
    * @param files files to include with the action
    * @param jobXmlOption optional job.xml path
    * @param configuration configuration to provide to the action
    * @param yarnConfig the yarn configuration
    * @param prepareOption an optional prepare step
    */
  def sqoopAction(name: String,
                  configuration: Configuration,
                  yarnConfig: YarnConfig,
                  prepareOption: Option[ActionPrepare],
                  jobXmlOption: Option[String],
                  command: String,
                  files: Seq[String])(implicit credentialsOption: Option[Credentials]): Node =
    SqoopAction(name, Some(command), Seq.empty, files, jobXmlOption, configuration, yarnConfig, prepareOption)

  /**
    * Oozie Sqoop action definition
    * @param name the name of the action
    * @param args arguments to specify to sqoop (ignored if command is specified)
    * @param files files to include with the action
    * @param jobXmlOption optional job.xml path
    * @param configuration configuration to provide to the action
    * @param yarnConfig the yarn configuration
    * @param prepareOption an optional prepare step
    */
  def sqoopAction(name: String,
                  configuration: Configuration,
                  yarnConfig: YarnConfig,
                  prepareOption: Option[ActionPrepare],
                  jobXmlOption: Option[String],
                  args: Seq[String],
                  files: Seq[String])(implicit credentialsOption: Option[Credentials]): Node =
    SqoopAction(name, None, args, files, jobXmlOption, configuration, yarnConfig, prepareOption)

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
                  propagateConfiguration: Boolean)(implicit credentialsOption: Option[Credentials]): Node =
    SubWorkflowAction(name, applicationPath, propagateConfiguration, configuration, yarnConfig)

  /**
    * Ooozie decision node switch
    *
    * @param node the node to switch to
    * @param predicate the predicate for switching to the node
    */
  def switch(node: Node, predicate: String): Switch = Switch(node, predicate)
}
// $COVERAGE-ON$
