package org.antipathy.scoozie.configuration

/**
  * Wrapper class for named properties
  */
private[scoozie] case class ActionProperties(config: Configuration, properties: Map[String, String])
