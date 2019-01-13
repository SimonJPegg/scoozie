package org.antipathy.scoozie.workflow

import org.antipathy.scoozie.{Nameable, Node, OozieProperties, XmlSerializable}
import org.antipathy.scoozie.configuration.{
  Configuration,
  Credentials,
  YarnConfig
}

import scala.xml.Elem
import org.antipathy.scoozie.control._
import org.antipathy.scoozie.validator.OozieValidator
import org.antipathy.scoozie.formatter.OozieXmlFormatter
import org.antipathy.scoozie.validator.SchemaType

import scala.collection.immutable._

/**
  * Oozie workflow definition
  * @param name the name of the workflow
  * @param transitions the actions within the workflow
  * @param credentialsOption optional credentials for this workflow
  * @param configurationOption optional configuration for this workflow
  * @param yarnConfig The yarn configuration for this workflow
  */
case class Workflow(
    override val name: String,
    transitions: Node,
    configurationOption: Option[Configuration] = None,
    yarnConfig: YarnConfig
)(implicit credentialsOption: Option[Credentials])
    extends XmlSerializable
    with Nameable {

  private val formatter = new OozieXmlFormatter(80, 4)

  private val (mappedCredentials, mappedCredProps) =
    credentialsOption.map(_.withActionProperties(name)) match {
      case Some((credentials, props)) => (credentials, props)
      case None =>
        import org.antipathy.scoozie.configuration.Credential
        (Credentials(Credential("", "", Seq())), Map())
    }

  private val (mappedConfig, mappedProperties) = configurationOption match {
    case Some(configuration) =>
      configuration.withActionProperties(name)
    case None => (Configuration(Seq.empty), Map())
  }

  /**
    * Validate this workflow produces valid Oozie XML
    */
  def validate(): Unit =
    OozieValidator.validate(formatter.format(this), SchemaType.workflow)

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
        Seq(n.toXML) ++ f.transitionPaths.flatMap(buildWorkflowXML)
      case d: Decision =>
        Seq(n.toXML) ++ d.transitionPaths.flatMap(buildWorkflowXML)
      case j: Join =>
        Seq(n.toXML) ++ buildWorkflowXML(j.transitionTo)
      case _: End => Seq.empty
      case _ =>
        val ok: Option[Seq[Elem]] = n._transition.map(buildWorkflowXML)
        val error: Option[Seq[Elem]] = n._failure.map(buildWorkflowXML)
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
        val ok = n._transition.map(buildWorkflowProperties)
        val error = n._failure.map(buildWorkflowProperties)
        n.properties ++ ok.getOrElse(Seq.empty) ++ error.getOrElse(Seq.empty)
    }

  /**
    * Get the Oozie properties for this object
    */
  private def properties: Map[String, String] =
    mappedCredProps ++ mappedProperties ++ buildWorkflowProperties(transitions)

  def jobProperties: String = {
    val pattern = "\\w+".r
    properties.flatMap {
      case (name, value) => pattern.findFirstIn(name).map(p => s"$p=$value")
    }.toSeq.sorted.toSet.mkString(System.lineSeparator())
  }

}
