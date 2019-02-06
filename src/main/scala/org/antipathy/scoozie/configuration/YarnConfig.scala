package org.antipathy.scoozie.configuration

import org.antipathy.scoozie.properties.OozieProperties
import scala.collection.immutable._

/**
  * Config for yarn Actions
  * @param jobTracker Job tracker URL
  * @param nameNode name node url
  */
case class YarnConfig(jobTracker: String, nameNode: String) extends OozieProperties {

  /**
    * Get the XML for the jobTracker property
    */
  private[scoozie] def jobTrackerXML =
    <job-tracker>{formatProperty("jobTracker")}</job-tracker>

  /**
    * Get the XML for the nameNode property
    */
  private[scoozie] def nameNodeXML =
    <name-node>{formatProperty("nameNode")}</name-node>

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(formatProperty("jobTracker") -> jobTracker, formatProperty("nameNode") -> nameNode)
}
