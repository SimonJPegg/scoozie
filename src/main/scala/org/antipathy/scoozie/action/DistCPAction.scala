package org.antipathy.scoozie.action

import com.typesafe.config.Config
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants, MonadBuilder, PrepareBuilder}
import org.antipathy.scoozie.configuration.{Arg, Configuration, Credentials, YarnConfig}
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.xml.Elem

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
                   override val configuration: Configuration,
                   yarnConfig: YarnConfig,
                   override val prepareOption: Option[Prepare])
    extends Action
    with HasPrepare
    with HasConfig {

  private val argumentsProperties = buildSequenceProperties(name, "arguments", arguments)
  private val javaOptionsProperty = formatProperty(s"${name}_javaOptions")

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
  mappedProperties ++
  prepareProperties

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <distcp xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {prepareXML}
      {configXML}
      {if (!javaOptions.isEmpty) {
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
    MonadBuilder.tryOperation[Node] { () =>
      DistCPAction(name = config.getString(HoconConstants.name),
                   arguments = Seq(config.getStringList(HoconConstants.arguments).asScala: _*),
                   javaOptions = config.getString(HoconConstants.javaOptions),
                   configuration = ConfigurationBuilder.buildConfiguration(config),
                   yarnConfig = yarnConfig,
                   prepareOption = PrepareBuilder.build(config))
    } { s: String =>
      new ConfigurationMissingException(s"$s in ${config.getString(HoconConstants.name)}")
    }
}
