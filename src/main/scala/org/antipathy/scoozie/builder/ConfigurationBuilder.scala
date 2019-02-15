/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.configuration.{Configuration, Credential, Credentials}
import org.antipathy.scoozie.exception.ConfigurationMissingException

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

/**
  *  Object for building configuration objects from Hocon
  */
private[scoozie] object ConfigurationBuilder {

  /**
    * Get the value at the specified path from the passed in config
    * @param config the config to get the value from
    * @param name the name of the key to get
    * @return An optional value
    */
  def optionalString(config: Config, name: String): Option[String] =
    if (config.hasPath(name)) {
      Some(config.getString(name))
    } else None

  /**
    * Get the value at the specified path from the passed in config
    * @param config the config to get the value from
    * @param name the name of the key to get
    * @return An optional value
    */
  def optionalBoolean(config: Config, name: String): Boolean =
    if (config.hasPath(name)) {
      config.getBoolean(name)
    } else false

  /**
    * Get the value at the specified path from the passed in config
    * @param config the config to get the value from
    * @param name the name of the key to get
    * @return a string collection
    */
  def optionalStringCollection(config: Config, name: String): Seq[String] =
    if (config.hasPath(name)) {
      Seq(config.getStringList(name).asScala: _*)
    } else Seq.empty[String]

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
    MonadBuilder.tryOperation[String] { () =>
      config.getString(path)
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${HoconConstants.credentials}", e)
    }
}
