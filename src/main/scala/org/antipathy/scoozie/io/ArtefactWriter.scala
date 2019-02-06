package org.antipathy.scoozie.io

import better.files._

/**
  * Trait for writing oozie artefacts to file
  */
private[scoozie] trait ArtefactWriter {

  /**
    * Write the passed in contents to the specified folder and file name
    *
    * @param outputFolder The folder to write to
    * @param fileName the file name to write to
    * @param contents the file contents to write
    */
  def writeFile(outputFolder: String, fileName: String, contents: String): Unit = {
    import java.nio.file.Paths
    val outputDir = File(Paths.get(outputFolder))

    val outputFile = outputDir / fileName
    outputFile.createFileIfNotExists(createParents = true)

    outputFile.writeText(contents)
  }
}

private[scoozie] object ArtefactWriter {
  val workflowFileName = "workflow.xml"
  val coordinatorFileName = "coordinator.xml"
  val propertiesFileName = "job.properties"
}
