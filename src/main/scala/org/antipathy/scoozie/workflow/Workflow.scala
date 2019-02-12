package org.antipathy.scoozie.workflow

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.action.{Nameable, Node, SubWorkflowAction}
import org.antipathy.scoozie.configuration._
import org.antipathy.scoozie.properties.{JobProperties, OozieProperties}
import org.antipathy.scoozie.xml.XmlSerializable

import scala.collection.immutable._
import scala.language.existentials
import scala.xml.Elem

/**
  * Oozie workflow definition
  * @param name the name of the workflow
  * @param path The path to this workflow
  * @param transitions the actions within the workflow
  * @param jobXmlOption optional job.xml path
  * @param credentialsOption optional credentials for this workflow
  * @param configuration configuration for this workflow
  * @param yarnConfig The yarn configuration for this workflow
  */
case class Workflow(override val name: String,
                    path: String,
                    transitions: Node,
                    jobXmlOption: Option[String],
                    configuration: Configuration,
                    yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials])
    extends XmlSerializable
    with Nameable
    with OozieProperties
    with JobProperties {

  private val jobXmlProperty =
    buildStringOptionProperty(name, "jobXml", jobXmlOption)

  private val (mappedCredentials, mappedCredProps) =
    credentialsOption.map(_.withActionProperties(name)) match {
      case Some(ActionProperties(credentials, props)) => (credentials, props)
      case None =>
        (Credentials(Credential("", "", Seq())), Map())
    }

  private val ActionProperties(mappedConfig, mappedProperties) = configuration.withActionProperties(name)

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <workflow-app name={name} xmlns="uri:oozie:workflow:0.5">
      <global>
        {yarnConfig.jobTrackerXML}
        {yarnConfig.nameNodeXML}
        {if (jobXmlOption.isDefined) {
          <job-xml>{jobXmlProperty.keys}</job-xml>
          }
        }
        {if (mappedConfig.configProperties.nonEmpty) {
            mappedConfig.toXML
          }
        }
      </global>
      {if (credentialsOption.isDefined) {
          mappedCredentials.toXML
        }
      }
      {buildWorkflowXML(transitions)}
      {End().toXML}
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
    mappedCredProps ++ mappedProperties ++ buildWorkflowProperties(transitions) ++ jobXmlProperty

  override def jobProperties: String = {
    val pattern = "\\w+".r
    properties.flatMap {
      case (pName, pValue) => pattern.findFirstIn(pName).map(p => s"$p=$pValue")
    }.toSet.toSeq.sorted.mkString(System.lineSeparator())
  }

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
