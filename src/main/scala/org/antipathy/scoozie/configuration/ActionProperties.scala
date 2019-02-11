package org.antipathy.scoozie.configuration

/**
  * Wrapper class for named properties
  */
private[scoozie] case class ActionProperties[T](mappedType: T, properties: Map[String, String])
