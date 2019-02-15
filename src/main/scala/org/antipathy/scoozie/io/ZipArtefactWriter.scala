package org.antipathy.scoozie.io

import java.io.File
import java.nio.file.{Files, Path, Paths}

import org.zeroturnaround.zip.{ByteSource, ZipEntrySource, ZipUtil}

import scala.collection.immutable.Seq

/**
  *  Trait for storing artefacts as zip files
  */
private[scoozie] trait ZipArtefactWriter {

  /**
    * Write the passed in artefacts as a zip file
    * @param path the path to writ to
    * @param filename the file name to write to
    * @param zipFileContents the artefacts to write
    */
  def writeZipFile(path: Path, filename: String, zipFileContents: Seq[Artefact]): Unit = {

    val fileName = Paths.get(path.toString + File.separator + filename)

    if (Files.exists(fileName)) { Files.delete(fileName) }

    //create empty zip archive
    Files.createDirectories(path)
    Files.createFile(fileName)
    ZipUtil.pack(Array.empty[ZipEntrySource], fileName.toFile)

    //populate it
    val contents: Array[ZipEntrySource] = zipFileContents.map { artefact =>
      new ByteSource(artefact.fileName, artefact.fileContents.getBytes())
    }.toArray
    ZipUtil.addEntries(fileName.toFile, contents)
  }
}
