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
package org.antipathy.scoozie.action.filesystem

import org.antipathy.scoozie.properties.PropertyFormatter

import scala.xml.Elem

/**
  * Create an ozzie move step
  * @param srcPath the path to move from
  * @param targetPath the path to move to
  */
case class Move(srcPath: String, targetPath: String) extends FileSystemAction with PropertyFormatter {

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <move source={formatProperty(srcPath)} target={formatProperty(targetPath)} />
}
