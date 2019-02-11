package org.antipathy.scoozie.coordinator

import org.antipathy.scoozie.action.Nameable
import org.antipathy.scoozie.configuration.Configuration
import org.antipathy.scoozie.properties.{JobProperties, OozieProperties}
import org.antipathy.scoozie.workflow.Workflow
import org.antipathy.scoozie.xml.XmlSerializable

import scala.collection.immutable._
import scala.language.existentials
import scala.xml.Elem

/**
  * Oozie coOrdinator definition
  * @param name the CoOrdinator name
  * @param frequency the CoOrdinator frequency
  * @param start the CoOrdinator start time
  * @param end the CoOrdinator end time
  * @param timezone the CoOrdinator time-zone
  * @param workflow the workflow to run
  * @param configuration configuration for the workflow
  */
case class Coordinator(override val name: String,
                       frequency: String,
                       start: String,
                       end: String,
                       timezone: String,
                       workflow: Workflow,
                       configuration: Configuration)
    extends XmlSerializable
    with Nameable
    with OozieProperties
    with JobProperties {

  private val (mappedConfig, mappedProperties) = configuration.withActionProperties(name)

  private val frequencyProperty = formatProperty(s"${name}_frequency")
  private val startProperty = formatProperty(s"${name}_start")
  private val endProperty = formatProperty(s"${name}_end")
  private val timezoneProperty = formatProperty(s"${name}_timezone")
  private val workflowPathProperty = formatProperty(s"${name}_workflow_path")

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(frequencyProperty -> frequency,
        startProperty -> start,
        endProperty -> end,
        timezoneProperty -> timezone,
        workflowPathProperty -> workflow.path) ++
    mappedProperties

  /**
    * Get the job properties
    */
  override def jobProperties: String = {
    val pattern = "\\w+".r
    properties.flatMap {
      case (pName, pValue) => pattern.findFirstIn(pName).map(p => s"$p=$pValue")
    }.toSet.toSeq.sorted.mkString(System.lineSeparator())
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <coordinator-app name={name}
                     frequency={frequencyProperty}
                     start={startProperty}
                     end={endProperty}
                     timezone={timezoneProperty}
                     xmlns="uri:oozie:coordinator:0.4">
      <action>
        <workflow>
          <app-path>{workflowPathProperty}</app-path>
          {if (mappedConfig.configProperties.nonEmpty) {
              mappedConfig.toXML
            }
          }
        </workflow>
      </action>
    </coordinator-app>

}
