package org.antipathy.scoozie.action.control

import org.antipathy.scoozie.action.{Action, Node}
import scala.xml.Elem
import scala.collection.immutable.Map

/**
  * oozie Start control node
  */
final class Start extends Action {

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
  override def name: String = "start"

  /**
    * The XML for this node
    */
  override def toXML: Elem = <unused />
}

object Start {

  def apply(): Node = Node(new Start())(None)
}
