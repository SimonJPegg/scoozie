package org.antipathy.scoozie.coordinator
import org.antipathy.scoozie.{Nameable, XmlSerializable}
import org.antipathy.scoozie.configuration.Configuration
import org.antipathy.scoozie.workflow.Workflow

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
case class CoOrdinator(override val name: String,
                       frequency: String,
                       start: String,
                       end: String,
                       timezone: String,
                       workflow: Workflow,
                       configuration: Configuration)
    extends XmlSerializable
    with Nameable {

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <coordinator-app name={name}
                     frequency={frequency}
                     start={start}
                     end={end}
                     timezone={timezone}
                     xmlns="uri:oozie:coordinator:0.1">
      <action>
        <workflow>
          <app-path>{workflow.path}</app-path>
          {if (configuration.configProperties.nonEmpty) {
              configuration.toXML
            }
          }
        </workflow>
      </action>
    </coordinator-app>

}
