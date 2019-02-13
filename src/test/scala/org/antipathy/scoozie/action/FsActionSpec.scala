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
