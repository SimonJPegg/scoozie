// $COVERAGE-OFF$
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
