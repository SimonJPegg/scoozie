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
package org.antipathy.scoozie.workflow

import com.typesafe.config.Config
import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.action._
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.builder._
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.exception.InvalidConfigurationException
import org.antipathy.scoozie.io.ArtefactWriter
import org.antipathy.scoozie.properties.{JobProperties, OozieProperties}
import org.antipathy.scoozie.sla.{HasSLA, OozieSLA}
import org.antipathy.scoozie.xml.XmlSerializable

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.language.existentials
import scala.xml.Elem

/**
  * Oozie workflow definition
  *
  * @param name the name of the workflow
  * @param path The path to this workflow
  * @param transitions the actions within the workflow
  * @param jobXmlOption optional job.xml path
  * @param credentialsOption optional credentials for this workflow
  * @param configuration configuration for this workflow
  * @param yarnConfig The yarn configuration for this workflow
  * @param slaOption Optional SLA for this workflow
  */
case class Workflow(override val name: String,
                    path: String,
                    transitions: Node,
                    jobXmlOption: Option[String],
                    configuration: Configuration,
                    yarnConfig: YarnConfig,
                    slaOption: Option[OozieSLA] = None)(implicit credentialsOption: Option[Credentials])
    extends XmlSerializable
    with Nameable
    with OozieProperties
    with JobProperties
    with HasJobXml
    with HasConfig
    with HasSLA {

  private val shareLib: String = "oozie.use.system.libpath=true"

  private val (mappedCredentials, mappedCredProps) =
    credentialsOption.map(_.withActionProperties(name)) match {
      case Some(ActionProperties(credentials, props)) => (credentials, props)
      case None =>
        (Credentials(Credential("", "", Seq())), Map())
    }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <workflow-app name={name} xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2">
      <global>
        {yarnConfig.jobTrackerXML}
        {yarnConfig.nameNodeXML}
        {jobXml}
        {configXML}
      </global>
      {if (credentialsOption.isDefined) {
          mappedCredentials.toXML
        }
      }
      {buildWorkflowXML(transitions)}
      {End().toXML}
      {slaXML}
    </workflow-app>

  /**
    * Walk the transitions for this workflow and build the XML
    */
  private[workflow] def buildWorkflowXML(n: Node): Seq[Elem] =
    n.action match {
      case f: Fork =>
        Seq(n.toXML) ++ f.transitionPaths.map(_.toXML) ++
        f.transitionPaths.flatMap(n => buildWorkflowXML(n))
      case d: Decision =>
        Seq(n.toXML) ++ d.transitionPaths.map(_.toXML) ++
        d.transitionPaths.flatMap(n => buildWorkflowXML(n))
      case j: Join =>
        Seq(n.toXML) ++ buildWorkflowXML(j.transitionTo)
      case _: End => Seq.empty
      case _ =>
        val ok: Option[Seq[Elem]] = n.successTransition.map(buildWorkflowXML)
        val error: Option[Seq[Elem]] = n.failureTransition.map(buildWorkflowXML)
        (Seq(n.toXML) ++ ok.getOrElse(Seq.empty) ++ error.getOrElse(Seq.empty)).distinct
    }

  private def buildWorkflowProperties(n: Node): Map[String, String] =
    n.action match {
      case f: Fork =>
        n.properties ++ f.transitionPaths.flatMap(buildWorkflowProperties)
      case d: Decision =>
        n.properties ++ d.transitionPaths.flatMap(buildWorkflowProperties)
      case j: Join =>
        buildWorkflowProperties(j.transitionTo)
      case _: End => Map()
      case _ =>
        val ok = n.successTransition.map(buildWorkflowProperties)
        val error = n.failureTransition.map(buildWorkflowProperties)
        n.properties ++ ok.getOrElse(Seq.empty) ++ error.getOrElse(Seq.empty)
    }

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    mappedCredProps ++ mappedProperties ++ buildWorkflowProperties(transitions) ++ jobXmlProperty ++ slaProperties

  override def jobProperties: String = {
    val pattern = "\\w+".r
    properties.flatMap {
      case (pName, pValue) => pattern.findFirstIn(pName).map(p => s"$p=$pValue")
    }.toSet.toSeq.sorted.mkString(System.lineSeparator())
  } ++ System.lineSeparator() + shareLib ++ System.lineSeparator() +
  s"oozie.wf.application.path=$path/${ArtefactWriter.workflowFileName}"

  /**
    * Convert this workflow into a subworkflow
    */
  def toSubWorkFlow(propagateConfiguration: Boolean): Node =
    if (propagateConfiguration) {
      SubWorkflowAction(name = this.name,
                        applicationPath = this.path,
                        propagateConfiguration = propagateConfiguration,
                        configuration = Scoozie.Configuration.emptyConfig,
                        yarnConfig = yarnConfig)
    } else {
      SubWorkflowAction(name = this.name,
                        applicationPath = this.path,
                        propagateConfiguration = propagateConfiguration,
                        configuration = this.configuration,
                        yarnConfig = yarnConfig)
    }
}

/**
  * Companion object
  */
object Workflow {

  /**
    *  Build a workflow from the passed in config
    * @param config the config to build from
    * @return a workflow
    */
  def apply(config: Config): Workflow =
    MonadBuilder.tryOperation[Workflow] { () =>
      val workflowConfig = config.getConfig(HoconConstants.workflow)

      implicit val credentials: Option[Credentials] =
        ConfigurationBuilder.buildCredentials(workflowConfig)

      val yarnConfig =
        YarnConfig(nameNode = workflowConfig.getString(s"${HoconConstants.yarnConfig}.${HoconConstants.nameNode}"),
                   jobTracker = workflowConfig.getString(s"${HoconConstants.yarnConfig}.${HoconConstants.jobTracker}"))

      val workFlowName = workflowConfig.getString(HoconConstants.name)

      Workflow(name = workFlowName,
               path = workflowConfig.getString(HoconConstants.path),
               transitions =
                 TransitionBuilder.build(Seq(workflowConfig.getConfigList(HoconConstants.transitions).asScala: _*),
                                         yarnConfig),
               jobXmlOption = ConfigurationBuilder.optionalString(workflowConfig, HoconConstants.jobXml),
               configuration = ConfigurationBuilder.buildConfiguration(workflowConfig),
               yarnConfig,
               slaOption = SLABuilder.buildSLA(workflowConfig, workFlowName))
    } { e: Throwable =>
      new InvalidConfigurationException(s"${e.getMessage} in workflow", e)
    }
}
