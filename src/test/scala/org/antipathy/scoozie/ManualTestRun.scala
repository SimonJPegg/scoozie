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
package org.antipathy.scoozie

import better.files.File
import org.scalatest.{FlatSpec, Matchers}

class ManualTestRun extends FlatSpec with Matchers {

  it should "build IT test artefacts" in {

    //docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 8888:8888 -p 7180:7180 -p 8020:8020 4239cd2958c6 /usr/bin/docker-quickstart

    val itInputPath = File("src/test/resources/conf/it/")
    val configPath = itInputPath / "it.conf"
    val outputPath = File("src/test/resources/output/manualTest/")

    outputPath.createIfNotExists(asDirectory = true)

    val artefacts = Scoozie.fromConfig(configPath.path)
    itInputPath.list.foreach(file => file.copyTo(outputPath / file.name, overwrite = true))
    artefacts.save(outputPath.path)
  }
}
