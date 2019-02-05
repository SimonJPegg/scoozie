package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.filesystem._
import scala.xml.Elem
import org.antipathy.scoozie.Node
import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.exception.UnknownActionException

/**
  * Oozie filesystem action definition
  * @param name the name of the action
  * @param actions the actions to perform
  */
class FsAction(override val name: String, actions: Seq[FileSystemAction]) extends Action {

  private val namedActionsAnProps: Seq[(FileSystemAction, Map[String, String])] = actions.zipWithIndex.map {
    case (Chmod(path, permissions, dirFiles), index) =>
      val p = s"${name}_chmodPath$index"
      val perm = s"${name}_chmodPermissions$index"
      val dir = s"${name}_chmodDirFiles$index"
      (Chmod(p, perm, dir), Map(p -> path, perm -> permissions, dir -> dirFiles))
    case (Delete(path), index) =>
      val p = s"${name}_deletePath$index"
      (Delete(p), Map(p -> path))
    case (MakeDir(path), index) =>
      val p = s"${name}_mkDirPath$index"
      (MakeDir(p), Map(p -> path))
    case (Move(srcPath, targetPath), index) =>
      val src = s"${name}_moveSrcPath$index"
      val dest = s"${name}_moveTargetPath$index"
      (Move(src, dest), Map(src -> srcPath, dest -> targetPath))
    case (Touchz(path), index) =>
      val p = s"${name}_touchzPath$index"
      (Touchz(p), Map(p -> path))
    case unknown =>
      throw new UnknownActionException(s"${unknown.getClass.getSimpleName} is not an expected FileSystem operation")
  }

  /**
    * The XML namespace for an action element
    */
  override def xmlns: Option[String] = None

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] = namedActionsAnProps.flatMap(_._2).toMap

  /**
    * The XML for this node
    */
  override def toXML: Elem = <fs>
    {namedActionsAnProps.map(_._1.toXML)}
  </fs>
}

object FsAction {
  def apply(name: String, actions: Seq[FileSystemAction])(implicit credentialsOption: Option[Credentials]): Node =
    Node(new FsAction(name, actions))
}
