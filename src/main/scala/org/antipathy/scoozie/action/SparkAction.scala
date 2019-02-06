package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration._
import scala.xml.Elem
import scala.collection.immutable._

/**
  * Oozie Spark action definition
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
final class SparkAction(override val name: String,
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
                        configuration: Configuration,
                        yarnConfig: YarnConfig)
    extends Action {

  private val sparkSettingsProperty = formatProperty(s"${name}_sparkSettings")
  private val sparkMasterURLProperty = formatProperty(s"${name}_sparkMasterURL")
  private val sparkModeProperty = formatProperty(s"${name}_sparkMode")
  private val sparkJobNameProperty = formatProperty(s"${name}_sparkJobName")
  private val mainClassProperty = formatProperty(s"${name}_mainClass")
  private val sparkJarProperty = formatProperty(s"${name}_sparkJar")
  private val sparkOptionsProperty = formatProperty(s"${name}_sparkOptions")
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
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:spark-action:1.0")

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(sparkSettingsProperty -> sparkSettings,
        sparkMasterURLProperty -> sparkMasterURL,
        sparkModeProperty -> sparkMode,
        sparkJobNameProperty -> sparkJobName,
        mainClassProperty -> mainClass,
        sparkJarProperty -> sparkJar,
        sparkOptionsProperty -> sparkOptions) ++
    commandLineArgsProperties ++
    prepareProperties ++
    filesProperties ++
    mappedConfigAndProperties._2

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <spark xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {if (prepareOptionMapped.isDefined) {
          prepareOptionMapped.get.toXML
        }
      }
      <job-xml>{sparkSettingsProperty}</job-xml>
      {if (mappedConfig.configProperties.nonEmpty) {
         mappedConfig.toXML
        }
      }
      <master>{sparkMasterURLProperty}</master>
      <mode>{sparkModeProperty}</mode>
      <name>{sparkJobNameProperty}</name>
      <class>{mainClassProperty}</class>
      <jar>{sparkJarProperty}</jar>
      <spark-opts>{sparkOptionsProperty}</spark-opts>
      {commandLineArgsProperties.keys.map(Arg(_).toXML)}
      {filesProperties.keys.map(File(_).toXML)}
    </spark>
}

object SparkAction {

  def apply(name: String,
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
            configuration: Configuration,
            yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new SparkAction(name,
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
    )
}
