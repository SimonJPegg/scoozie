package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.properties.OozieProperties

import scala.collection.immutable._

/**
  * Config for yarn Actions
  * @param jobTracker Job tracker URL
  * @param nameNode name node url
  */
case class YarnConfig(jobTracker: String, nameNode: String) extends OozieProperties {

  private val jobTrackerName: String = "jobTracker"
  private val nameNodeName: String = "nameNode"

  /**
    * Get the XML for the jobTracker property
    */
  private[scoozie] def jobTrackerXML =
    <job-tracker>{formatProperty(jobTrackerName)}</job-tracker>

  /**
    * Get the XML for the nameNode property
    */
  private[scoozie] def nameNodeXML =
    <name-node>{formatProperty(nameNodeName)}</name-node>

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(formatProperty(jobTrackerName) -> jobTracker, formatProperty(nameNodeName) -> nameNode)
}
