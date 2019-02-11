package org.antipathy.scoozie.action

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.configuration.{Credentials, YarnConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class PigActionSpec extends FlatSpec with Matchers {

  behavior of "PigAction"

  it should "generate valid XML" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = PigAction(name = "pigAction",
                           script = "/path/to/script",
                           params = Seq(),
                           arguments = Seq(),
                           files = Seq(),
                           jobXmlOption = Some("/path/to/job.xml"),
                           configuration = Scoozie.Configuration.emptyConfig,
                           yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode"),
                           prepareOption = None).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<pig>
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${pigAction_jobXml}"}</job-xml>
          <script>{"${pigAction_script}"}</script>
        </pig>))

    result.properties should be(
      Map("${pigAction_script}" -> "/path/to/script", "${pigAction_jobXml}" -> "/path/to/job.xml")
    )
  }

  it should "generate valid XML with script arguments" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = PigAction(name = "pigAction",
                           script = "/path/to/script",
                           params = Seq("pone", "ptwo"),
                           arguments = Seq("aone", "atwo"),
                           files = Seq("fone", "ftwo"),
                           jobXmlOption = None,
                           configuration = Scoozie.Configuration.emptyConfig,
                           yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode"),
                           prepareOption = None).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<pig>
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <script>{"${pigAction_script}"}</script>
        <param>{"${pigAction_param0}"}</param>
        <param>{"${pigAction_param1}"}</param>
        <argument>{"${pigAction_arg0}"}</argument>
        <argument>{"${pigAction_arg1}"}</argument>
        <file>{"${pigAction_file0}"}</file>
        <file>{"${pigAction_file1}"}</file>
      </pig>))

    result.properties should be(
      Map("${pigAction_file1}" -> "ftwo",
          "${pigAction_param0}" -> "pone",
          "${pigAction_script}" -> "/path/to/script",
          "${pigAction_arg0}" -> "aone",
          "${pigAction_arg1}" -> "atwo",
          "${pigAction_param1}" -> "ptwo",
          "${pigAction_file0}" -> "fone")
    )
  }
}
