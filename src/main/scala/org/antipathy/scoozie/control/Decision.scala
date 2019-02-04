package org.antipathy.scoozie.control

import org.antipathy.scoozie.Node
import org.antipathy.scoozie.action.Action
import scala.xml.Elem
import scala.collection.immutable
import scala.collection.immutable._

/**
  * Oozie decision control node
  * @param name the name of the decision node
  * @param switches the switches in the decision node
  * @param default the default action for the decision
  */
final class Decision(override val name: String,
                     default: Node,
                     switches: Seq[Switch])
    extends Action {

  /**
    * Get the Oozie properties for this object
    */
  override val properties: Map[String, String] = Map()

  /**
    * The default path for this node
    */
  def defaultPath: Node = default

  /**
    * The nodes contained within this fork
    */
  def transitionPaths: Seq[Node] = switches.map(_.node) :+ default

  /**
    * The XML namespace for this action element
    */
  override val xmlns: Option[String] = None

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <decision name={name}>
      <switch>
        {switches.map(_.toXML)}
        <default to={default.action.name}/>
      </switch>
    </decision>
}

object Decision {

  def apply(name: String, default: Node, switch: Switch*): Node =
    Node(new Decision(name, default, immutable.Seq(switch.toSeq: _*)))(None)

  def apply(name: String, default: Node, switches: Seq[Switch]): Node =
    Node(new Decision(name, default, switches))(None)
}
