package org.antipathy.scoozie.coordinator

import com.typesafe.config.Config
import org.antipathy.scoozie.action.{HasConfig, Nameable}
import org.antipathy.scoozie.builder._
import org.antipathy.scoozie.configuration.Configuration
import org.antipathy.scoozie.exception.InvalidConfigurationException
import org.antipathy.scoozie.properties.{JobProperties, OozieProperties}
import org.antipathy.scoozie.sla.{HasSLA, OozieSLA}
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
  * @param slaOption Optional SLA for this coordinator
  */
case class Coordinator(override val name: String,
                       frequency: String,
                       start: String,
                       end: String,
                       timezone: String,
                       workflow: Workflow,
                       configuration: Configuration,
                       slaOption: Option[OozieSLA] = None)
    extends XmlSerializable
    with Nameable
    with OozieProperties
    with JobProperties
    with HasConfig
    with HasSLA {

  //private val ActionProperties(mappedConfig, mappedProperties) = configuration.withActionProperties(name)

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
    mappedProperties ++
    slaProperties

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
                     xmlns="uri:oozie:coordinator:0.4" xmlns:sla="uri:oozie:sla:0.2">
      <action>
        <workflow>
          <app-path>{workflowPathProperty}</app-path>
          {configXML}
        </workflow>
        {slaXML}
      </action>
    </coordinator-app>
}

/**
  * Companion object
  */
object Coordinator {

  /**
    *  Build a Coordinator from the passed in config
    * @param config the config to build from
    * @return a Coordinator
    */
  def apply(config: Config): Coordinator =
    MonadBuilder.tryOperation { () =>
      val coordinatorConfig = config.getConfig(HoconConstants.coordinator)
      val coordinatorName = coordinatorConfig.getString(HoconConstants.name)
      Coordinator(name = coordinatorName,
                  frequency = coordinatorConfig.getString(HoconConstants.frequency),
                  start = coordinatorConfig.getString(HoconConstants.start),
                  end = coordinatorConfig.getString(HoconConstants.end),
                  timezone = coordinatorConfig.getString(HoconConstants.timezone),
                  workflow = WorkflowBuilder.build(config),
                  configuration = ConfigurationBuilder.buildConfiguration(coordinatorConfig),
                  slaOption = SLABuilder.buildSLA(coordinatorConfig, coordinatorName))
    } { e: Throwable =>
      new InvalidConfigurationException(s"${e.getMessage} in coordinator", e)
    }
}
