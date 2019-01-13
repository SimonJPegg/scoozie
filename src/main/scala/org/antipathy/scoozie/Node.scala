package org.antipathy.scoozie

import org.antipathy.scoozie.action.Action
import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.control._
import scala.xml.Elem
import scala.collection.immutable.Map

/**
  * Wapper class for oozie actions, used to define transitions
  *
  * @param action the inner oozie action
  * @param _transition the node to transition to on success
  * @param _failure the node to tranistion to on failure
  * @param credentialsOption optional credentials for the nodes
  */
private[scoozie] case class Node(
    action: Action,
    _transition: Option[Node] = None,
    _failure: Option[Node] = None
)(implicit credentialsOption: Option[Credentials])
    extends XmlSerializable
    with OozieProperties {

  /**
    * The node to transition to on success
    */
  def okTo(node: Node): Node = this.copy(_transition = Some(node))

  /**
    * The node to transition to on failure
    */
  def errorTo(node: Node): Node = this.copy(_failure = Some(node))

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
    if (_transition.isEmpty) {
      throw new IllegalArgumentException(
        "No node has been defined to start from"
      )
    } else {
      <start to={_transition.get.action.name} />
    }

  /**
    * validate a element has transitions and build it
    */
  private def buildActionXML: Elem = {
    if (_transition.isEmpty) {
      throw new IllegalArgumentException(
        s"${action.name} does not have an okTo set"
      )
    }

    if (_failure.isEmpty) {
      throw new IllegalArgumentException(
        s"${action.name} does not have an errorTo set"
      )
    }

    <action name ={action.name}
      cred={credentialsOption.map(_.credential.name).orNull}>
      {action.toXML}
      <ok to= {_transition.get.action.name}/>
      <error to = {_failure.get.action.name}/>
    </action>
  }

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] = action.properties
}
