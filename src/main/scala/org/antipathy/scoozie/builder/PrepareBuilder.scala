/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.exception.UnknownStepException

import scala.collection.JavaConverters._
import scala.collection.immutable._

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
    if (config.hasPath(HoconConstants.prepare) &&
        config.getConfig(HoconConstants.prepare).entrySet().asScala.nonEmpty) {
      val steps = config
        .getConfig(HoconConstants.prepare)
        .entrySet()
        .asScala
        .toSeq
        .sortBy(_.getKey)
        .map {
          case delete if delete.getKey.toLowerCase.equals(HoconConstants.delete) => Delete(delete.getValue.render())
          case mkdir if mkdir.getKey.toLowerCase.equals(HoconConstants.mkDir)    => MakeDir(mkdir.getValue.render())
          case unknown =>
            throw new UnknownStepException(s"$unknown is not a valid ${HoconConstants.prepare} step")
        }
      Some(Prepare(Seq(steps: _*)))
    } else {
      None
    }
}
