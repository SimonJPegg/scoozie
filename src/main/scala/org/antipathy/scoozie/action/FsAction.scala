package org.antipathy.scoozie.action

import java.util

import com.typesafe.config.Config
import org.antipathy.scoozie.action.filesystem._
import org.antipathy.scoozie.builder.ConfigurationBuilder
import org.antipathy.scoozie.configuration.Configuration
import org.antipathy.scoozie.exception.{ConfigurationMissingException, UnknownActionException, UnknownStepException}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.util._
import scala.xml.Elem

/**
  * Oozie filesystem action definition
  * @param name the name of the action
  * @param steps the actions to perform
  * @param jobXmlOption optional job.xml path
  * @param configuration additional config for this action
  */
class FsAction(override val name: String,
               steps: Seq[FileSystemAction],
               jobXmlOption: Option[String],
               configuration: Configuration)
    extends Action {

  private val jobXmlProperty = buildStringOptionProperty(name, "jobXml", jobXmlOption)

  private val namedActionsAnProps: Seq[(FileSystemAction, Map[String, String])] = steps.zipWithIndex.map {
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
  override def properties: Map[String, String] =
    jobXmlProperty ++ namedActionsAnProps.flatMap(_._2).toMap

  /**
    * The XML for this node
    */
  override def toXML: Elem = <fs>
    {if (jobXmlOption.isDefined) {
      <job-xml>{jobXmlProperty.keys}</job-xml>
      }
    }
    {namedActionsAnProps.map(_._1.toXML)}
  </fs>
}

/**
  * Companion object
  */
object FsAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            actions: Seq[FileSystemAction],
            jobXmlOption: Option[String],
            configuration: Configuration): Node =
    Node(new FsAction(name, actions, jobXmlOption, configuration))(None)

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config): Node =
    Try {
      FsAction(name = config.getString("name"),
               actions = buildFSSteps(config.getConfigList("steps")),
               jobXmlOption = if (config.hasPath("job-xml")) {
                 Some(config.getString("job-xml"))
               } else None,
               configuration = ConfigurationBuilder.buildConfiguration(config))
    } match {
      case Success(node) => node
      case Failure(exception) =>
        throw new ConfigurationMissingException(s"${exception.getMessage} in ${config.getString("name")}")
    }

  /**
    * Build the steps in this action from a collection of configuration objects
    */
  private def buildFSSteps(configList: util.List[_ <: Config]): Seq[FileSystemAction] =
    Seq(configList.asScala.map {
      case delete if delete.hasPath("delete") => Delete(delete.getString("delete"))
      case mkdir if mkdir.hasPath("mkdir")    => MakeDir(mkdir.getString("mkdir"))
      case touchz if touchz.hasPath("touchz") => Touchz(touchz.getString("touchz"))
      case chmod if chmod.hasPath("chmod") =>
        Chmod(chmod.getString("chmod.path"), chmod.getString("chmod.permissions"), chmod.getString("chmod.dir-files"))
      case move if move.hasPath("move") =>
        Move(move.getString("move.source"), move.getString("move.target"))
      case unknown =>
        throw new UnknownStepException(s"$unknown is not a valid filesystem step")
    }: _*)

}
