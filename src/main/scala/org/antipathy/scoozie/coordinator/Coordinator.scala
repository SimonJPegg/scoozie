/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
package org.antipathy.scoozie.coordinator

import com.typesafe.config.Config
import org.antipathy.scoozie.action.{HasConfig, Nameable}
import org.antipathy.scoozie.builder._
import org.antipathy.scoozie.configuration.Configuration
import org.antipathy.scoozie.exception.InvalidConfigurationException
import org.antipathy.scoozie.io.ArtefactWriter
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
  * @param path the HDFS path of the coordinator
  * @param frequency the CoOrdinator frequency
  * @param start the CoOrdinator start time
  * @param end the CoOrdinator end time
  * @param timezone the CoOrdinator time-zone
  * @param workflow the workflow to run
  * @param configuration configuration for the workflow
  * @param slaOption Optional SLA for this coordinator
  */
case class Coordinator(override val name: String,
                       path: String,
                       frequency: String,
                       start: String,
                       end: String,
                       timezone: String,
                       workflow: Workflow,
                       configuration: Configuration,
                       slaOption: Option[OozieSLA] = None)
    extends XmlSerializable
    with Nameable
    with HasConfig
    with OozieProperties
    with JobProperties
    with HasSLA {

  private val startProperty = formatProperty(s"${name}_start")
  private val endProperty = formatProperty(s"${name}_end")
  private val timezoneProperty = formatProperty(s"${name}_timezone")
  private val workflowPathProperty = formatProperty(s"${name}_workflow_path")

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(startProperty -> start, endProperty -> end, timezoneProperty -> timezone, workflowPathProperty -> workflow.path) ++
    actionConfig.properties ++
    slaProperties

  /**
    * Get the job properties
    */
  override def jobProperties: String = {
    val pattern = "\\w+".r
    properties.flatMap {
      case (pName, pValue) => pattern.findFirstIn(pName).map(p => s"$p=$pValue")
    }.toSet.toSeq.sorted.mkString(System.lineSeparator()) + System.lineSeparator() +
    s"oozie.coord.application.path=$path/${ArtefactWriter.coordinatorFileName}"
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <coordinator-app name={name}
                     frequency={formatProperty(frequency)}
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
                  path = coordinatorConfig.getString(HoconConstants.path),
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
