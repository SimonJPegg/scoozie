package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration._

import scala.xml.Elem
import org.antipathy.scoozie.Node

import scala.collection.immutable._

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
final class JavaAction(override val name: String,
                       mainClass: String,
                       javaJar: String,
                       javaOptions: String,
                       commandLineArgs: Seq[String],
                       files: Seq[String],
                       captureOutput: Boolean,
                       configuration: Configuration,
                       yarnConfig: YarnConfig,
                       prepareOption: Option[Prepare] = None)
    extends Action {

  private val mainClassProperty = formatProperty(s"${name}_mainClass")
  private val javaJarProperty = formatProperty(s"${name}_javaJar")
  private val javaOptionsProperty = formatProperty(s"${name}_javaOptions")
  private val commandLineArgsProperties =
    buildSequenceProperties(name, "commandLineArg", commandLineArgs)
  private val filesProperties = buildSequenceProperties(name, "files", files)
  private val prepareOptionAndProps =
    prepareOption.map(_.withActionProperties(name))
  private val prepareProperties =
    prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)
  private val mappedConfigAndProperties = configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties._1

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(mainClassProperty -> mainClass, javaJarProperty -> javaJar, javaOptionsProperty -> javaOptions) ++
    commandLineArgsProperties ++
    prepareProperties ++
    filesProperties ++
    mappedConfigAndProperties._2

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <java>
        {yarnConfig.jobTrackerXML}
        {yarnConfig.nameNodeXML}
        {if (prepareOptionMapped.isDefined) {
            prepareOptionMapped.get.toXML
          }
        }
        {if (mappedConfig.configProperties.nonEmpty) {
            mappedConfig.toXML
          }
        }
        <main-class>{mainClassProperty}</main-class>
        <java-opts>{javaOptionsProperty}</java-opts>
        {commandLineArgsProperties.keys.map(Arg(_).toXML)}
        {filesProperties.keys.map(File(_).toXML)}
        <file>{javaJarProperty}</file>
        { if (captureOutput) {
            <capture-output />
          }
        }
      </java>
}

object JavaAction {

  def apply(name: String,
            mainClass: String,
            javaJar: String,
            javaOptions: String,
            commandLineArgs: Seq[String],
            files: Seq[String],
            captureOutput: Boolean,
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new JavaAction(name,
                     mainClass,
                     javaJar,
                     javaOptions,
                     commandLineArgs,
                     files,
                     captureOutput,
                     configuration,
                     yarnConfig,
                     prepareOption)
    )
}
