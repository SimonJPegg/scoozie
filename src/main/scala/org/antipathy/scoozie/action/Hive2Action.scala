package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.{Configuration, Credentials, YarnConfig}

import scala.xml.Elem
import org.antipathy.scoozie.Node

import scala.collection.immutable._

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
final class Hive2Action(override val name: String,
                        hiveSettingsXML: String,
                        scriptName: String,
                        scriptLocation: String,
                        parameters: Seq[String],
                        configuration: Configuration,
                        yarnConfig: YarnConfig,
                        jdbcUrl: String,
                        password: Option[String] = None,
                        prepareOption: Option[Prepare] = None)
    extends Action {

  private val hiveSettingsXMLProperty = formatProperty(s"${name}_hiveSettingsXML")
  private val scriptNameProperty = formatProperty(s"${name}_scriptName")
  private val scriptLocationProperty = formatProperty(s"${name}_scriptLocation")
  private val jdbcUrlProperty = formatProperty(s"${name}_jdbcUrl")
  private val parametersProperties =
    buildSequenceProperties(name, "parameter", parameters)
  private val passwordProperty =
    buildStringOptionProperty(name, "password", password)
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
  override val xmlns: Option[String] = Some("uri:oozie:hive2-action:0.1")

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    yarnConfig.properties ++ Map(
      hiveSettingsXMLProperty -> hiveSettingsXML,
      scriptNameProperty -> scriptName,
      scriptLocationProperty -> scriptLocation,
      jdbcUrlProperty -> jdbcUrl
    ) ++ passwordProperty ++ prepareProperties ++ mappedConfigAndProperties._2

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <hive2 xmlns={xmlns.orNull}>
      {yarnConfig.jobTrackerXML}
      {yarnConfig.nameNodeXML}
      {if (prepareOptionMapped.isDefined) {
          prepareOptionMapped.get.toXML
        }
      }
      <job-xml>{hiveSettingsXMLProperty}</job-xml>
        {if (mappedConfig.configProperties.nonEmpty) {
          mappedConfig.toXML
        }
      }
      <jdbc-url>{jdbcUrlProperty}</jdbc-url>
      { if (passwordProperty.nonEmpty) {
          <password>{passwordProperty.keys.head}</password>
        }
      }
      <script>{scriptNameProperty}</script>
      {parametersProperties.map{
          case (key,_) => <param>{key}</param>
        }
      }
      <file>{scriptLocationProperty}</file>
    </hive2>
}

object Hive2Action {

  def apply(name: String,
            hiveSettingsXML: String,
            scriptName: String,
            scriptLocation: String,
            parameters: Seq[String],
            configuration: Configuration,
            yarnConfig: YarnConfig,
            jdbcUrl: String,
            password: Option[String] = None,
            prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
    Node(
      new Hive2Action(name,
                      hiveSettingsXML,
                      scriptName,
                      scriptLocation,
                      parameters,
                      configuration,
                      yarnConfig,
                      jdbcUrl,
                      password,
                      prepareOption)
    )
}
