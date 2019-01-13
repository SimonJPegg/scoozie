package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.YarnConfig
import scala.xml.Elem
import org.antipathy.scoozie.Node
import org.antipathy.scoozie.configuration.Credentials
import scala.collection.immutable._

/**
  * Oozie Hive action definition
  * @param name the name of the action
  * @param hiveSettingsXML the path to the hive settings XML
  * @param scriptName the name of the hive script
  * @param scriptLocation the path to the hive script
  * @param parameters a collection of parameters to the hive script
  * @param config Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
final class HiveAction(override val name: String,
                       hiveSettingsXML: String,
                       scriptName: String,
                       scriptLocation: String,
                       parameters: Seq[String],
                       config: YarnConfig,
                       prepareOption: Option[Prepare] = None)
    extends Action {

  private val hiveSettingsXMLProperty = formatProperty(
    s"${name}_hiveSettingsXML"
  )
  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val parametersProperties =
    buildSequenceProperties(name, "parameter", parameters)
  private val prepareOptionAndProps =
    prepareOption.map(_.withActionProperties(name))
  private val prepareProperties =
    prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)
  private val mappedConfigAndProperties =
    config.configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties._1

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    config.properties ++ Map(
      hiveSettingsXMLProperty -> hiveSettingsXML,
      scriptNameProperty -> scriptName,
      scriptLocationProperty -> scriptLocation
    ) ++ prepareProperties ++ parametersProperties ++ mappedConfigAndProperties._2

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:hive-action:0.2")

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <hive xmlns={xmlns.orNull}>
        {config.jobTrackerXML}
        {config.nameNodeXML}
        {if (prepareOptionMapped.isDefined) {
            prepareOptionMapped.map(_.toXML)
          }
        }
        <job-xml>{hiveSettingsXMLProperty}</job-xml>
        {if (mappedConfig.configProperties.nonEmpty) {
            mappedConfig.toXML
          }
        }
        <script>{scriptNameProperty}</script>
        {parametersProperties.map{
            case (key,_) => <param>{key}</param>
          }
        }
        <file>{scriptLocationProperty}</file>
      </hive>
}

object HiveAction {

  def apply(
      name: String,
      hiveSettingsXML: String,
      scriptName: String,
      scriptLocation: String,
      parameters: Seq[String],
      config: YarnConfig,
      prepareOption: Option[Prepare] = None
  )(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new HiveAction(name,
                     hiveSettingsXML,
                     scriptName,
                     scriptLocation,
                     parameters,
                     config,
                     prepareOption)
    )
}
