package org.antipathy.scoozie.coordinator
import org.antipathy.scoozie.{JobProperties, Nameable, XmlSerializable}
import org.antipathy.scoozie.configuration.Configuration
import org.antipathy.scoozie.workflow.Workflow
import scala.collection.immutable._
import scala.xml.Elem

/**
  * Oozie coOrdinator definition
  * @param name the CoOrdinator name
  * @param frequency the CoOrdinator frequency
  * @param start the CoOrdinator start time
  * @param end the CoOrdinator end time
  * @param timezone the CoOrdinator time-zone
  * @param workflow the workflow to run
  * @param configurationOption optional configuration for the workflow
  */
case class CoOrdinator(override val name: String,
                       frequency: String,
                       start: String,
                       end: String,
                       timezone: String,
                       workflow: Workflow,
                       configurationOption: Option[Configuration] = None)
    extends XmlSerializable
    with Nameable
    with JobProperties {

  private val (mappedConfig, mappedProperties) = configurationOption match {
    case Some(configuration) =>
      configuration.withActionProperties(name)
    case None =>
      (Configuration(Seq.empty), Map())
  }

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
