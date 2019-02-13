// $COVERAGE-OFF$
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
