package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.configuration.{Configuration, Credential, Credentials}
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.util._

/**
  *  Object for building configuration objects from Hocon
  */
private[scoozie] object ConfigurationBuilder {

  /**
    * Build a configuration object from the passed in config file
    * @param config the configuration to build from
    * @return a configuration object
    */
  def buildConfiguration(config: Config): Configuration =
    if (config.hasPath(HoconConstants.configuration)) {
      Scoozie.Configuration.configuration(
        config
          .getConfig(HoconConstants.configuration)
          .entrySet()
          .asScala
          .toSeq
          .sortBy(_.getKey)
          .map { i =>
            i.getKey -> i.getValue.render()
          }
          .toMap
      )
    } else Scoozie.Configuration.emptyConfig

  /**
    * Build a credentials object from the passed in config file
    * @param config the config to build from
    * @return an optional credentials object
    */
  def buildCredentials(config: Config): Option[Credentials] =
    if (config.hasPath(HoconConstants.credentials)) {
      val credentialsConfig = config.getConfig(HoconConstants.credentials)
      Some(
        Credentials(
          Credential(configStringValue(credentialsConfig, HoconConstants.name),
                     configStringValue(credentialsConfig, HoconConstants.typ),
                     buildConfiguration(credentialsConfig).configProperties)
        )
      )
    } else None

  /**
    * wrap missing keys with a more helpful message
    */
  private def configStringValue(config: Config, path: String): String =
    Try {
      config.getString(path)
    } match {
      case Success(value) => value
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${HoconConstants.credentials}")
    }
}
