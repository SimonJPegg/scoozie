package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.action.prepare.Prepare
import scala.collection.immutable._
import scala.collection.JavaConverters._
import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}
import org.antipathy.scoozie.exception.UnknownStepException

/**
  * object for building prepare steps for actions
  */
private[scoozie] object PrepareBuilder {

  /**
    * build a prepare step for an action from the passed in config
    * @param config the config to build from
    * @return a prepare object
    */
  def build(config: Config): Option[Prepare] =
    if (config.hasPath("prepare") &&
        config.getConfig("prepare").entrySet().asScala.nonEmpty) {
      val steps = config
        .getConfig("prepare")
        .entrySet()
        .asScala
        .map {
          case delete if delete.getKey.toLowerCase.equals("delete") => Delete(delete.getValue.render())
          case mkdir if mkdir.getKey.toLowerCase.equals("mkdir")    => MakeDir(mkdir.getValue.render())
          case unknown =>
            throw new UnknownStepException(s"$unknown is not a valid prepare step")
        }
        .toSeq
      Some(Prepare(Seq(steps: _*)))
    } else {
      None
    }
}
