package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.builder.{ConfigurationBuilder, PrepareBuilder}
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.util._
import scala.xml.Elem

/**
  * Oozie Java action definition
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
final class JavaAction(override val name: String,
                       mainClass: String,
                       javaJar: String,
                       javaOptions: String,
                       commandLineArgs: Seq[String],
                       files: Seq[String],
                       captureOutput: Boolean,
                       jobXmlOption: Option[String],
                       configuration: Configuration,
                       yarnConfig: YarnConfig,
                       prepareOption: Option[Prepare])
    extends Action {

  private val mainClassProperty = formatProperty(s"${name}_mainClass")
  private val javaJarProperty = formatProperty(s"${name}_javaJar")
  private val javaOptionsProperty = formatProperty(s"${name}_javaOptions")
  private val commandLineArgsProperties =
    buildSequenceProperties(name, "commandLineArg", commandLineArgs)
  private val filesProperties = buildSequenceProperties(name, "files", files)
  private val jobXmlProperty =
    buildStringOptionProperty(name, "jobXml", jobXmlOption)
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
    mappedConfigAndProperties._2 ++ jobXmlProperty

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
        {prepareOptionMapped.map(_.toXML).orNull}
        {if (jobXmlOption.isDefined) {
            <job-xml>{jobXmlProperty.keys}</job-xml>
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

/**
  * Companion object
  */
object JavaAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            mainClass: String,
            javaJar: String,
            javaOptions: String,
            commandLineArgs: Seq[String],
            files: Seq[String],
            captureOutput: Boolean,
            jobXmlOption: Option[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new JavaAction(name,
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
    )

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    Try {
      JavaAction(name = config.getString("name"),
                 mainClass = config.getString("main-class"),
                 javaJar = config.getString("java-jar"),
                 javaOptions = config.getString("java-options"),
                 commandLineArgs = Seq(config.getStringList("command-line-arguments").asScala: _*),
                 files = Seq(config.getStringList("files").asScala: _*),
                 captureOutput = if (config.hasPath("capture-output")) {
                   config.getBoolean("capture-output")
                 } else false,
                 jobXmlOption = if (config.hasPath("job-xml")) {
                   Some(config.getString("job-xml"))
                 } else None,
                 configuration = ConfigurationBuilder.buildConfiguration(config),
                 yarnConfig,
                 prepareOption = PrepareBuilder.build(config))
    } match {
      case Success(node) => node
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString("name")}")
    }
}
