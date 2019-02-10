package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.{Configuration, YarnConfig}
import scala.collection.immutable._
import org.antipathy.scoozie.configuration.Arg
import org.antipathy.scoozie.configuration.Credentials
import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{ConfigurationBuilder, PrepareBuilder}
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception.ConfigurationMissingException

/**
  * DistCP action definition
  * @param name the name of the action
  * @param arguments arguments to the DistCP action
  * @param javaOptions java options to pass to the action
  * @param configuration additional configuration to pass to the action
  * @param yarnConfig the yarn configuration
  * @param prepareOption optional preparation step
  */
class DistCPAction(override val name: String,
                   arguments: Seq[String],
                   javaOptions: String,
                   configuration: Configuration,
                   yarnConfig: YarnConfig,
                   prepareOption: Option[Prepare])
    extends Action {
  import scala.xml.Elem

  private val argumentsProperties = buildSequenceProperties(name, "arguments", arguments)
  private val javaOptionsProperty = formatProperty(s"${name}_javaOptions")

  private val configurationProperties = configuration.withActionProperties(name)
  private val prepareOptionAndProps = prepareOption.map(_.withActionProperties(name))
  private val prepareProperties = prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:distcp-action:0.2")

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] =
  Map(javaOptionsProperty -> javaOptions) ++
  argumentsProperties ++
  configurationProperties._2 ++
  prepareProperties

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <distcp xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {if (prepareOptionMapped.isDefined) {
          prepareOptionMapped.get.toXML
         }
      }
      {if (configurationProperties._1.configProperties.nonEmpty) {
          configurationProperties._1.toXML
        }
      }
      {if (javaOptions.length > 0) {
          <java-opts>{javaOptionsProperty}</java-opts>
        }
      }
      {argumentsProperties.keys.map(Arg(_).toXML)}
    </distcp>
}

/**
  * Companion object
  */
object DistCPAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            arguments: Seq[String],
            javaOptions: String,
            configuration: Configuration,
            yarnConfig: YarnConfig,
            prepareOption: Option[Prepare])(implicit credentialsOption: Option[Credentials]): Node =
    Node(new DistCPAction(name, arguments, javaOptions, configuration, yarnConfig, prepareOption))

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config, yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node =
    try {
      DistCPAction(name = config.getString("name"),
                   arguments = Seq(config.getStringList("arguments").asScala: _*),
                   javaOptions = config.getString("java-options"),
                   configuration = ConfigurationBuilder.buildConfiguration(config),
                   yarnConfig = yarnConfig,
                   prepareOption = PrepareBuilder.build(config))
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }

}
