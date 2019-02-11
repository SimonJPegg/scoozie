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
