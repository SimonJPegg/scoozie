// $COVERAGE-OFF$
package org.antipathy.scoozie.api

import org.antipathy.scoozie.configuration.{
  Credential,
  Credentials,
  Property,
  YarnConfig,
  Configuration => ActionConfiguration
}

import scala.collection.immutable.{Map, Seq}

/**
  * Methods for creating Oozie properties
  */
object Configuration {

  /**
    * Create an oozie property
    * @param name the name of the property
    * @param value the value of the property
    * @return an Oozie property
    */
  def property(name: String, value: String): Property = Property(name, value)

  /**
    * Oozie configuration for a workflow or an action
    * @param properties the properties of the configuration
    * @return an oozie configuration
    */
  def configuration(properties: Seq[Property]): ActionConfiguration =
    ActionConfiguration(properties)

  /**
    * Oozie configuration for a workflow or an action
    * @param properties a map of oozie properties
    * @return
    */
  def configuration(properties: Map[String, String]): ActionConfiguration =
    ActionConfiguration(Seq(properties.map {
      case (key, value) => Property(key, value)
    }.toSeq: _*))

  /**
    * Empty Oozie configuration for a workflow or an action
    */
  def emptyConfig: ActionConfiguration = ActionConfiguration(Seq.empty)

  /**
    * Create the credentials for an oozie workflow
    * @param name the name of the credential
    * @param credentialsType the type of the credential
    * @param properties the credential's properties
    * @return
    */
  def credentials(name: String, credentialsType: String, properties: Seq[Property]): Option[Credentials] =
    Some(Credentials(Credential(name, credentialsType, properties)))

  /**
    * Create a set of empty credentials for an oozie workflow
    * @return
    */
  def emptyCredentials: Option[Credentials] = None

  /**
    * Create a yarn configuration for an oozie workflow
    *
    * @param jobTracker The oozie job tracker
    * @param nameNode The HDFS name node
    * @return a yarn configuration
    */
  def yarnConfig(jobTracker: String, nameNode: String) =
    YarnConfig(jobTracker, nameNode)
}
// $COVERAGE-ON$
