package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants, PrepareBuilder}
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
    prepareOptionAndProps.map(_.properties).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_.mappedType)
  private val mappedConfigAndProperties = configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties.mappedType

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(mainClassProperty -> mainClass, javaJarProperty -> javaJar, javaOptionsProperty -> javaOptions) ++
    commandLineArgsProperties ++
    prepareProperties ++
    filesProperties ++
    mappedConfigAndProperties.properties ++ jobXmlProperty

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
      JavaAction(name = config.getString(HoconConstants.name),
                 mainClass = config.getString(HoconConstants.mainClass),
                 javaJar = config.getString(HoconConstants.javaJar),
                 javaOptions = config.getString(HoconConstants.javaOptions),
                 commandLineArgs = Seq(config.getStringList(HoconConstants.commandLineArguments).asScala: _*),
                 files = Seq(config.getStringList(HoconConstants.files).asScala: _*),
                 captureOutput = if (config.hasPath(HoconConstants.captureOutput)) {
                   config.getBoolean(HoconConstants.captureOutput)
                 } else { false },
                 jobXmlOption = if (config.hasPath(HoconConstants.jobXml)) {
                   Some(config.getString(HoconConstants.jobXml))
                 } else None,
                 configuration = ConfigurationBuilder.buildConfiguration(config),
                 yarnConfig,
                 prepareOption = PrepareBuilder.build(config))
    } match {
      case Success(node) => node
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString(HoconConstants.name)}")
    }
}
