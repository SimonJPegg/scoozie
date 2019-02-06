package org.antipathy.scoozie.action.prepare

import scala.xml.Elem
import scala.collection.immutable.Map
import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}
import org.antipathy.scoozie.xml.XmlSerializable

/**
  * Ooize actions prepare definition
  * @param actions the prepare actions
  */
case class Prepare(actions: Seq[PrepareFSAction]) extends XmlSerializable {

  /**
    * Copy this action substituting the values for property names
    * @param actionName the name of the action calling this method
    * @return a copy of the action and its properties
    */
  private[scoozie] def withActionProperties(actionName: String): (Prepare, Map[String, String]) = {
    val mappedProps = actions.map {
      case d: Delete =>
        val p = "${" + s"${actionName}_prepare_delete" + "}"
        (Delete(p), p -> d.path)
      case m: MakeDir =>
        val p = "${" + s"${actionName}_prepare_makedir" + "}"
        (MakeDir(p), p -> m.path)
    }
    (this.copy(mappedProps.map(_._1)), mappedProps.map(_._2).toMap)
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <prepare>
      {actions.map(_.toXML)}
    </prepare>
}
