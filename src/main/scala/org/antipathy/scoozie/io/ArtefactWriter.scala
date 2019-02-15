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
