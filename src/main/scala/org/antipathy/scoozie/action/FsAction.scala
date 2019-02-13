package org.antipathy.scoozie.action

import java.util

import com.typesafe.config.Config
import org.antipathy.scoozie.action.filesystem._
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants, MonadBuilder}
import org.antipathy.scoozie.configuration.{ActionProperties, Configuration}
import org.antipathy.scoozie.exception.{ConfigurationMissingException, UnknownActionException, UnknownStepException}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
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
               override val configuration: Configuration)
    extends Action
    with HasConfig {

  private val jobXmlProperty = buildStringOptionProperty(name, "jobXml", jobXmlOption)

  private val namedActionsAnProps: Seq[ActionProperties[FileSystemAction]] = steps.zipWithIndex.map {
    case (Chmod(path, permissions, dirFiles), index) =>
      val p = s"${name}_chmodPath$index"
      val perm = s"${name}_chmodPermissions$index"
      val dir = s"${name}_chmodDirFiles$index"
      ActionProperties[FileSystemAction](Chmod(p, perm, dir), Map(p -> path, perm -> permissions, dir -> dirFiles))
    case (Delete(path), index) =>
      val p = s"${name}_deletePath$index"
      ActionProperties[FileSystemAction](Delete(p), Map(p -> path))
    case (MakeDir(path), index) =>
      val p = s"${name}_mkDirPath$index"
      ActionProperties[FileSystemAction](MakeDir(p), Map(p -> path))
    case (Move(srcPath, targetPath), index) =>
      val src = s"${name}_moveSrcPath$index"
      val dest = s"${name}_moveTargetPath$index"
      ActionProperties[FileSystemAction](Move(src, dest), Map(src -> srcPath, dest -> targetPath))
    case (Touchz(path), index) =>
      val p = s"${name}_touchzPath$index"
      ActionProperties[FileSystemAction](Touchz(p), Map(p -> path))
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
    jobXmlProperty ++ namedActionsAnProps.flatMap(_.properties).toMap ++ configurationProperties.properties

  /**
    * The XML for this node
    */
  override def toXML: Elem = <fs>
    {if (jobXmlOption.isDefined) {
      <job-xml>{jobXmlProperty.keys}</job-xml>
      }
    }
    {if (mappedConfig.configProperties.nonEmpty) {
        mappedConfig.toXML
      }
    }
    {namedActionsAnProps.map(_.mappedType.toXML)}
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
    MonadBuilder.tryOperation[Node] { () =>
      FsAction(name = config.getString(HoconConstants.name),
               actions = buildFSSteps(config.getConfigList(HoconConstants.steps)),
               jobXmlOption = ConfigurationBuilder.optionalString(config, HoconConstants.jobXml),
               configuration = ConfigurationBuilder.buildConfiguration(config))
    } { e: Throwable =>
      new ConfigurationMissingException(s"${e.getMessage} in ${config.getString(HoconConstants.name)}", e)
    }

  /**
    * Build the steps in this action from a collection of configuration objects
    */
  private def buildFSSteps(configList: util.List[_ <: Config]): Seq[FileSystemAction] =
    Seq(configList.asScala.map {
      case delete if delete.hasPath(HoconConstants.delete) => Delete(delete.getString(HoconConstants.delete))
      case mkdir if mkdir.hasPath(HoconConstants.mkDir)    => MakeDir(mkdir.getString(HoconConstants.mkDir))
      case touchz if touchz.hasPath(HoconConstants.touchz) => Touchz(touchz.getString(HoconConstants.touchz))
      case chmod if chmod.hasPath(HoconConstants.chmod) =>
        Chmod(chmod.getString(s"${HoconConstants.chmod}.${HoconConstants.path}"),
              chmod.getString(s"${HoconConstants.chmod}.${HoconConstants.permissions}"),
              chmod.getString(s"${HoconConstants.chmod}.${HoconConstants.dirFiles}"))
      case move if move.hasPath(s"${HoconConstants.move}") =>
        Move(move.getString(s"${HoconConstants.move}.${HoconConstants.source}"),
             move.getString(s"${HoconConstants.move}.${HoconConstants.target}"))
      case unknown =>
        throw new UnknownStepException(s"$unknown is not a valid filesystem step")
    }: _*)

}
