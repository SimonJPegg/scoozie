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
package org.antipathy.scoozie.traits

import java.nio.file.Path

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.io.{Artefact, ArtefactWriter, ZipArtefactWriter}
import org.antipathy.scoozie.properties.JobProperties
import org.antipathy.scoozie.workflow.Workflow

import scala.collection.immutable.Seq

/**
  * Trait that may be extended by clients of the library
  */
trait ScoozieWorkflow extends JobProperties with ArtefactWriter with ZipArtefactWriter {

  /**
    * Get the workflow generated by this class
    */
  def workflow: Workflow

  /**
    * Get the job properties for this workflow
    */
  override def jobProperties: String = this.workflow.jobProperties

  /**
    * Writes this workflow and its properties to the specified folder.
    * The folder will be created if it does not exist
    *
    * file names on output will be `workflow.xml` and `job.properties`
    *
    * @param location the location to write to
    */
  def save(location: Path, asZipFile: Boolean = false): Unit = {
    Scoozie.Test.validate(this.workflow)
    val artefacts = Seq(Artefact(ArtefactWriter.workflowFileName, Scoozie.Formatting.format(this.workflow)),
                        Artefact(ArtefactWriter.propertiesFileName, this.jobProperties))
    if (asZipFile) {
      writeZipFile(location, ArtefactWriter.zipArchive, artefacts)
    } else {
      artefacts.foreach(writeFile(location, _))
    }
  }
}