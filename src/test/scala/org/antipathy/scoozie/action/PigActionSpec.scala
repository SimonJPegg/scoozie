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
