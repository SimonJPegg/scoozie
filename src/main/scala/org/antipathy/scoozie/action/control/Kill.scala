package org.antipathy.scoozie.action.control

import org.antipathy.scoozie.action.{Action, Node}
import scala.xml.Elem
import scala.collection.immutable.Map

/**
  * Oozie kill control node
  */
final class Kill(message: String) extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The name of this element
    */
  override def name: String = "kill"

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <kill name={name}>
      <message>{message}</message>
    </kill>
}

object Kill {
  def apply(message: String): Node =
    Node(new Kill(message))(None)
}
