package org.antipathy.scoozie.action.control

import org.antipathy.scoozie.action.{Action, Node}
import org.antipathy.scoozie.exception.TransitionException

import scala.collection.immutable.Map
import scala.xml.Elem

/**
  * Oozie Fork control node
  * @param name the name of the fork
  * @param nodes the nodes within the fork
  */
final class Fork(override val name: String, nodes: Seq[Node]) extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The nodes contained within this fork
    */
  def transitionPaths: Seq[Node] = nodes

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem = {
    if (transitionPaths.length < 2) {
      throw new TransitionException(s"Error in Fork($name): must have at least 2 actions")
    }

    val pathsXml = transitionPaths.map(n => <path start={n.action.name} />)
    <fork name={name}>
        {pathsXml}
      </fork>
  }
}

object Fork {
  def apply(name: String, nodes: Seq[Node]): Node =
    Node(new Fork(name, nodes))(None)
}
