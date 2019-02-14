package org.antipathy.scoozie.action.prepare

import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}
import org.antipathy.scoozie.configuration.ActionProperties
import org.antipathy.scoozie.xml.XmlSerializable

import scala.collection.immutable._
import scala.xml.Elem

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
  private[scoozie] def withActionProperties(actionName: String): ActionProperties[Prepare] = {
    val mappedProps = actions.map {
      case d: Delete =>
        val p = Prepare.varPrefix + s"${actionName}_prepare_delete" + Prepare.varPostfix
        ActionProperties[PrepareFSAction](Delete(p), Map(p -> d.path.replace("\"", "")))
      case m: MakeDir =>
        val p = Prepare.varPrefix + s"${actionName}_prepare_makedir" + Prepare.varPostfix
        ActionProperties[PrepareFSAction](MakeDir(p), Map(p -> m.path.replace("\"", "")))
      case unknown =>
        throw new IllegalArgumentException(s"${unknown.getClass.getSimpleName} is not a valid prepare step")
    }
    ActionProperties(this.copy(mappedProps.map(_.mappedType)), mappedProps.flatMap(_.properties).toMap)
  }

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <prepare>
      {actions.map(_.toXML)}
    </prepare>
}

object Prepare {
  val varPrefix: String = "${"
  val varPostfix: String = "}"
}
