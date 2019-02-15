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
