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
package org.antipathy.scoozie.action

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.configuration.Credentials
import org.scalatest.{FlatSpec, Matchers}

class FsActionSpec extends FlatSpec with Matchers {

  behavior of "FsAction"

  it should "generate valid XML" in {

    implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials

    val actions = Seq(Scoozie.FileSystem.chmod("chmod1Path", "somepermissions1", "false"),
                      Scoozie.FileSystem.delete("someDeletePath"),
                      Scoozie.FileSystem.makeDirectory("someCreatePath"),
                      Scoozie.FileSystem.move("movefrompath", "moveToPath"),
                      Scoozie.FileSystem.touchz("someTouchPath"),
                      Scoozie.FileSystem.chmod("chmod2Path", "somepermissions2", "true"))
    val result = Scoozie.Actions.fs("fsAction", None, Scoozie.Configuration.emptyConfig, actions: _*).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<fs>
        <chmod path="${fsAction_chmodPath0}" permissions="${fsAction_chmodPermissions0}" dir-files="${fsAction_chmodDirFiles0}"/>
        <delete path="${fsAction_deletePath1}"/>
        <mkdir path="${fsAction_mkDirPath2}"/>
        <move source="${fsAction_moveSrcPath3}" target="${fsAction_moveTargetPath3}"/>
        <touchz path="${fsAction_touchzPath4}"/>
        <chmod path="${fsAction_chmodPath5}" permissions="${fsAction_chmodPermissions5}" dir-files="${fsAction_chmodDirFiles5}"/>
      </fs>))

    result.properties should be(
      Map("fsAction_chmodDirFiles0" -> "false",
          "fsAction_chmodDirFiles5" -> "true",
          "fsAction_moveSrcPath3" -> "movefrompath",
          "fsAction_chmodPermissions0" -> "somepermissions1",
          "fsAction_touchzPath4" -> "someTouchPath",
          "fsAction_mkDirPath2" -> "someCreatePath",
          "fsAction_chmodPath5" -> "chmod2Path",
          "fsAction_moveTargetPath3" -> "moveToPath",
          "fsAction_deletePath1" -> "someDeletePath",
          "fsAction_chmodPath0" -> "chmod1Path",
          "fsAction_chmodPermissions5" -> "somepermissions2")
    )
  }

}
