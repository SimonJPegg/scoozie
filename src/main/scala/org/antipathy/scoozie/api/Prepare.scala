// $COVERAGE-OFF$
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
package org.antipathy.scoozie.api

import org.antipathy.scoozie.action.filesystem.{Delete, MakeDir}
import org.antipathy.scoozie.action.prepare.{PrepareFSAction, Prepare => ActionPrepare}

import scala.collection.immutable.Seq

/**
  * Action preparation methods
  */
object Prepare {

  /**
    * Create a delete preparation step
    *
    * @param path the path to delete
    * @return a delete preparation step
    */
  def delete(path: String): Delete = Delete(path)

  /**
    * Create a make directory preparation step
    * @param path the path to create
    * @return a make directory preparation step
    */
  def makeDirectory(path: String): MakeDir = MakeDir(path)

  /**
    * Create an action preparation step
    * @param actions the preparation actions
    * @return an action preparation step
    */
  def prepare(actions: Seq[PrepareFSAction]): Option[ActionPrepare] =
    Some(ActionPrepare(actions))
}
// $COVERAGE-ON$
