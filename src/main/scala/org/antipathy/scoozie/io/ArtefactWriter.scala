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
package org.antipathy.scoozie.io

import java.nio.file.{Path, Paths}

import better.files._

/**
  * Trait for writing oozie artefacts to file
  */
private[scoozie] trait ArtefactWriter {

  /**
    * Write the passed in contents to the specified folder and file name
    *
    * @param outputFolder The folder to write to
    * @param artefact The artefact to write
    */
  protected def writeFile(outputFolder: Path, artefact: Artefact): Unit = {
    val outputDir = File(Paths.get(outputFolder.toString))

    val outputFile = outputDir / artefact.fileName
    outputFile.createIfNotExists(createParents = true)

    outputFile.writeText(artefact.fileContents)
  }
}

private[scoozie] object ArtefactWriter {
  val workflowFileName = "workflow.xml"
  val coordinatorFileName = "coordinator.xml"
  val propertiesFileName = "job.properties"
  val zipArchive = "artefacts.zip"
}
