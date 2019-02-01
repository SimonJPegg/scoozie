package org.antipathy.scoozie.control

import org.antipathy.scoozie.{Node, XmlSerializable}
import scala.xml.Elem

/**
  * Ooozie decision node switch
  *
  * @param node the node to switch to
  * @param predicate the predicate for switching to this node
  */
case class Switch(node: Node, predicate: String) extends XmlSerializable {

  /**
    * expected predicate pattern for oozie switches
    */
  private val Pattern = """[${].*[}]""".r

  /**
    * format predicates to expected pattern
    */
  private val formattedPredicate = predicate match {
    case Pattern() => predicate
    case _         => "${" + predicate + "}"
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <case to={node.action.name}>{formattedPredicate}</case>
}
