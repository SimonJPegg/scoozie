package org.antipathy.scoozie.action.control

import org.antipathy.scoozie.action.{Action, Node}
import scala.xml.Elem
import scala.collection.immutable.Map

/**
  * oozie end control node
  */
final class End extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The name of this element
    */
  override val name: String = "end"

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem = <end name={name} />

}

object End {
  def apply(): Node = Node(new End())(None)
}
