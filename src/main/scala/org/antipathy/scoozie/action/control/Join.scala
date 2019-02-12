package org.antipathy.scoozie.action.control

import org.antipathy.scoozie.action.{Action, Node}

import scala.collection.immutable.Map
import scala.xml.Elem

/**
  * Oozie join control node
  * @param name the name of the join
  * @param to the node the join transitions to
  */
final class Join(override val name: String, to: Node) extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The element to transition to on success
    */
  def transitionTo: Node = to

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <join name={name} to={to.action.name} />
}

object Join {
  def apply(name: String, to: Node): Node =
    Node(new Join(name, to))(None)
}
