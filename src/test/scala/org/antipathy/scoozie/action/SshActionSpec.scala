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

import org.antipathy.scoozie.configuration.Credentials
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class SshActionSpec extends FlatSpec with Matchers {

  behavior of "SshAction"

  it should "generate valid XML" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result =
      SshAction(name = "ssh", host = "user@host", command = "ls", args = Seq("-l", "-a"), captureOutput = true).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<ssh xmlns="uri:oozie:ssh-action:0.2">
          <host>{"${ssh_host}"}</host>
          <command>{"${ssh_command}"}</command>
          <args>{"${ssh_arg0}"}</args>
          <args>{"${ssh_arg1}"}</args>
          <capture-output/>
        </ssh>))

    result.properties should be(
      Map("${ssh_host}" -> "user@host", "${ssh_command}" -> "ls", "${ssh_arg0}" -> "-l", "${ssh_arg1}" -> "-a")
    )
  }
}
