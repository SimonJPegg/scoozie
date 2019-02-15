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

import java.nio.file.Path

import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{CoordinatorBuilder, TransitionStringBuilder, WorkflowBuilder}
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.exception.TransitionException
import org.antipathy.scoozie.io.{Artefact, ArtefactWriter, ZipArtefactWriter}
import org.antipathy.scoozie.workflow.Workflow

import scala.collection.immutable.Seq

/**
  * Container class for HOCON generated arefacts
  */
final class GeneratedArtefacts(workflow: Workflow,
                               coordinatorOption: Option[Coordinator],
                               validationStringOption: Option[String])
    extends ArtefactWriter
    with ZipArtefactWriter {

  /**
    * Validate the generated artefacts
    */
  def validate(): Unit = {
    Scoozie.Test.validate(workflow)
    coordinatorOption.foreach(Scoozie.Test.validate)

    validationStringOption.foreach { vString =>
      val traversalString = Scoozie.Test.workflowTesterWorkflowTestRunner(workflow).traversalPath
      if (!vString.equals(traversalString)) {
        throw new TransitionException(s"""Expected transition:
                                         |$vString
                                         |Actual transition:
                                         |$traversalString""".stripMargin)
      }
    }
  }

  /**
    * Save the generated artefacts to the specified path
    * @param outputPath the path to write to
    */
  def save(outputPath: Path, asZipFile: Boolean = false): Unit = {
    this.validate()

    val artefacts = coordinatorOption.map { c =>
      Seq(Artefact(ArtefactWriter.coordinatorFileName, Scoozie.Formatting.format(c)),
          Artefact(ArtefactWriter.propertiesFileName,
                   c.jobProperties + System.lineSeparator() + this.workflow.jobProperties
                     .replace("oozie.wf.application.path", "#oozie.wf.application.path")))
    }.getOrElse(Seq(Artefact(ArtefactWriter.propertiesFileName, this.workflow.jobProperties))) ++ Seq(
      Artefact(ArtefactWriter.workflowFileName, Scoozie.Formatting.format(this.workflow))
    )

    if (asZipFile) {
      writeZipFile(outputPath, ArtefactWriter.zipArchive, artefacts)
    } else {
      artefacts.foreach(writeFile(outputPath, _))
    }
  }

}

/**
  * Companion object
  */
object GeneratedArtefacts {

  /**
    * Generate a set of artefacts from the passed in config file
    * @param config the config file to generate from
    * @return a `GeneratedArtefacts` object
    */
  def apply(config: Config): GeneratedArtefacts = {

    val workflow: Workflow = WorkflowBuilder.build(config)

    val coordinatorOption: Option[Coordinator] = if (config.hasPath("coordinator")) {
      Some(CoordinatorBuilder.build(config))
    } else { None }

    val validationStringOption: Option[String] = if (config.hasPath("validate.transitions")) {
      Some(TransitionStringBuilder.build(config))
    } else { None }

    new GeneratedArtefacts(workflow, coordinatorOption, validationStringOption)
  }

}
