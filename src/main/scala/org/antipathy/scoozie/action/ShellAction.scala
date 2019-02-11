package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration._
import scala.xml.Elem
import org.antipathy.scoozie.configuration.Credentials
import scala.collection.immutable._
import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{ConfigurationBuilder, PrepareBuilder}
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception.ConfigurationMissingException

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
final class ShellAction(override val name: String,
                        scriptName: String,
                        scriptLocation: String,
                        commandLineArgs: Seq[String],
                        envVars: Seq[String],
                        files: Seq[String],
                        captureOutput: Boolean,
                        jobXmlOption: Option[String],
                        configuration: Configuration,
                        yarnConfig: YarnConfig,
                        prepareOption: Option[Prepare])
    extends Action {

  <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>

  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val commandLineArgsProperties =
    buildSequenceProperties(name, "commandLineArgs", commandLineArgs)
  private val envVarsProperties =
    buildSequenceProperties(name, "envVars", envVars)
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
    Map(scriptNameProperty -> scriptName, scriptLocationProperty -> scriptLocation) ++ prepareProperties ++
    commandLineArgsProperties ++
    envVarsProperties ++
    mappedConfigAndProperties._2 ++ jobXmlProperty ++ filesProperties

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:shell-action:0.2")

  /**
    * The XML for this node
    */
  override def toXML: Elem = {
    val fileVal = s"$scriptLocationProperty#$scriptNameProperty"
    <shell xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {if (prepareOptionMapped.isDefined) {
          prepareOptionMapped.get.toXML
        }
      }
      {if (jobXmlOption.isDefined) {
          <job-xml>{jobXmlProperty.keys}</job-xml>
        }
      }
      {if (mappedConfig.configProperties.nonEmpty) {
          mappedConfig.toXML
        }
      }
      <exec>{scriptNameProperty}</exec>
      {commandLineArgsProperties.keys.map(Argument(_).toXML)}
      {envVarsProperties.keys.map(EnvVar(_).toXML)}
      {filesProperties.keys.map(File(_).toXML)}
      <file>{fileVal}</file>
      { if (captureOutput) {
          <capture-output />
        }
      }
    </shell>
  }
}

/**
  * Companion object
  */
object ShellAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            scriptName: String,
            scriptLocation: String,
            commandLineArgs: Seq[String],
            envVars: Seq[String],
            files: Seq[String],
            captureOutput: Boolean,
            jobXmlOption: Option[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new ShellAction(name,
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
    )

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    try {
      ShellAction(name = config.getString("name"),
                  scriptName = config.getString("script-name"),
                  scriptLocation = config.getString("script-location"),
                  commandLineArgs = Seq(config.getStringList("command-line-arguments").asScala: _*),
                  envVars = Seq(config.getStringList("environment-variables").asScala: _*),
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
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
