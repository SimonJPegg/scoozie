package org.antipathy.scoozie.workflow

import org.antipathy.scoozie.{JobProperties, Nameable, Node, XmlSerializable}
import org.antipathy.scoozie.configuration.{Configuration, Credentials, YarnConfig}

import scala.language.existentials
import scala.xml.Elem
import org.antipathy.scoozie.control._
import org.antipathy.scoozie.validator.OozieValidator
import org.antipathy.scoozie.formatter.OozieXmlFormatter
import org.antipathy.scoozie.validator.SchemaType

import scala.collection.immutable._
import org.antipathy.scoozie.configuration.Credential

/**
  * Oozie workflow definition
  * @param name the name of the workflow
  * @param path The path to this workflow
  * @param transitions the actions within the workflow
  * @param credentialsOption optional credentials for this workflow
  * @param configurationOption optional configuration for this workflow
  * @param yarnConfig The yarn configuration for this workflow
  */
case class Workflow(override val name: String,
                    path: String,
                    transitions: Node,
                    configurationOption: Option[Configuration] = None,
                    yarnConfig: YarnConfig)(implicit credentialsOption: Option[Credentials])
    extends XmlSerializable
    with Nameable
    with JobProperties {
  import org.antipathy.scoozie.action.SubWorkflowAction

  private val formatter = new OozieXmlFormatter(80, 4)

  private val (mappedCredentials, mappedCredProps) =
    credentialsOption.map(_.withActionProperties(name)) match {
      case Some((credentials, props)) => (credentials, props)
      case None =>
        (Credentials(Credential("", "", Seq())), Map())
    }

  private val (mappedConfig, mappedProperties) = configurationOption match {
    case Some(configuration) =>
      configuration.withActionProperties(name)
    case None => (Configuration(Seq.empty), Map())
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <workflow-app name={name} xmlns="uri:oozie:workflow:0.4">
      <global>
        {yarnConfig.jobTrackerXML}
        {yarnConfig.nameNodeXML}
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
  private def properties: Map[String, String] =
    mappedCredProps ++ mappedProperties ++ buildWorkflowProperties(transitions)

  override def jobProperties: String = {
    val pattern = "\\w+".r
    properties.flatMap {
      case (pName, pValue) => pattern.findFirstIn(pName).map(p => s"$p=$pValue")
    }.toSeq.sorted.toSet.mkString(System.lineSeparator())
  }

  /**
    * Convert this workflow into a subworkflow
    */
  def toSubWorkFlow(propagateConfiguration: Boolean): Node =
    SubWorkflowAction(name = this.name,
                      applicationPath = this.path,
                      propagateConfiguration = propagateConfiguration,
                      config = this.yarnConfig)

}
