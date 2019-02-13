package org.antipathy.scoozie.action

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.exception.TransitionException
import org.antipathy.scoozie.properties.OozieProperties
import org.antipathy.scoozie.sla.{HasSLA, OozieSLA}
import org.antipathy.scoozie.xml.XmlSerializable

import scala.collection.immutable.Map
import scala.xml.Elem

/**
  * Wapper class for oozie actions, used to define transitions
  *
  * @param action the inner oozie action
  * @param successTransition the node to transition to on success
  * @param failureTransition the node to tranistion to on failure
  * @param credentialsOption optional credentials for the nodes
  */
private[scoozie] case class Node(action: Action,
                                 successTransition: Option[Node] = None,
                                 failureTransition: Option[Node] = None,
                                 slaOption: Option[OozieSLA] = None)(implicit credentialsOption: Option[Credentials])
    extends XmlSerializable
    with OozieProperties
    with Nameable
    with HasSLA {

  /**
    * The node to transition to on success
    */
  def okTo(node: Node): Node = this.action match {
    case _ @(_: Fork | _: Decision | _: End | _: Kill) => this
    case _                                             => this.copy(successTransition = Some(node))
  }

  /**
    * The node to transition to on failure
    */
  def errorTo(node: Node): Node = this.action match {
    case _ @(_: Fork | _: Decision | _: End | _: Kill | _: Start | _: Join) => this
    case _                                                                  => this.copy(failureTransition = Some(node))
  }

  /**
    *  Add an SLA to the action within this node.
    * @param oozieSLA The SLA to add
    * @return a node with an SLA.
    */
  def withSLA(oozieSLA: OozieSLA): Node = this.action match {
    case _ @(_: Fork | _: Decision | _: End | _: Kill | _: Start | _: Join) => this
    case _ =>
      this.copy(slaOption = Some(oozieSLA))
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    action match {
      case d: Decision => d.toXML
      case e: End      => e.toXML
      case f: Fork     => f.toXML
      case j: Join     => j.toXML
      case k: Kill     => k.toXML
      case _: Start    => buildStartXML
      case _           => buildActionXML
    }

  /**
    * Validate the start element has a transition and build it
    */
  private def buildStartXML: Elem =
    if (successTransition.isEmpty) {
      throw new TransitionException("No node has been defined to start from")
    } else {
      <start to={successTransition.map(_.action.name).orNull} />
    }

  /**
    * validate a element has transitions and build it
    */
  private def buildActionXML: Elem = {
    if (successTransition.isEmpty) {
      throw new TransitionException(s"${action.name} does not have an okTo set")
    }

    if (failureTransition.isEmpty) {
      throw new TransitionException(s"${action.name} does not have an errorTo set")
    }

    <action name ={action.name}
      cred={if (action.requiresCredentials) credentialsOption.map(_.credential.name).orNull else Scoozie.Null}>
      {action.toXML}
      <ok to= {successTransition.map(_.action.name).orNull} />
      <error to = {failureTransition.map(_.action.name).orNull}/>
      {slaXML}
    </action>
  }

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] = action.properties ++ slaProperties

  /**
    * The name of the object
    */
  override def name: String = action.name
}
