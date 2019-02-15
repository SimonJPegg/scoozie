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

import org.antipathy.scoozie.action.filesystem._

/**
  * Oozie filesystem operations
  */
object FileSystem {

  /**
    * Create an ozzie chmod step
    *
    * @param path the path to operate on
    * @param permissions the permissions to set
    * @param dirFiles should the operation be recursive
    */
  def chmod(path: String, permissions: String, dirFiles: String): Chmod = Chmod(path, permissions, dirFiles)

  /**
    * Create a delete step
    *
    * @param path the path to delete
    * @return a delete step
    */
  def delete(path: String): Delete = Delete(path)

  /**
    * Create a make directory step
    * @param path the path to create
    * @return a make directory step
    */
  def makeDirectory(path: String): MakeDir = MakeDir(path)

  /**
    * Create an ozzie move step
    * @param srcPath the path to move from
    * @param targetPath the path to move to
    */
  def move(srcPath: String, targetPath: String): Move = Move(srcPath, targetPath)

  /**
    * Create a touch step
    * @param path the path to touch
    * @return a touch step
    */
  def touchz(path: String): Touchz = Touchz(path)
}
// $COVERAGE-ON$
