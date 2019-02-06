package org.antipathy.scoozie.coordinator

import org.antipathy.scoozie.action.Nameable
import org.antipathy.scoozie.configuration.Configuration
import org.antipathy.scoozie.properties.JobProperties
import org.antipathy.scoozie.workflow.Workflow
import org.antipathy.scoozie.xml.XmlSerializable
import scala.xml.Elem
import scala.language.existentials

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
    with JobProperties {

  private val (mappedConfig, mappedProperties) = configuration.withActionProperties(name)

  /**
    * Get the job properties
    */
  override def jobProperties: String = {
    val pattern = "\\w+".r
    mappedProperties.flatMap {
      case (pName, pValue) => pattern.findFirstIn(pName).map(p => s"$p=$pValue")
    }.toSeq.sorted.toSet.mkString(System.lineSeparator()) +
    System.lineSeparator() +
    workflow.jobProperties

  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <coordinator-app name={name}
                     frequency={frequency}
                     start={start}
                     end={end}
                     timezone={timezone}
                     xmlns="uri:oozie:coordinator:0.4">
      <action>
        <workflow>
          <app-path>{workflow.path}</app-path>
          {if (mappedConfig.configProperties.nonEmpty) {
              mappedConfig.toXML
            }
          }
        </workflow>
      </action>
    </coordinator-app>

}
