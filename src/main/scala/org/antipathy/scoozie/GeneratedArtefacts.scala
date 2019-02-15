package org.antipathy.scoozie

import java.nio.file.Path

import com.typesafe.config.Config
import org.antipathy.scoozie.builder.{CoordinatorBuilder, TransitionStringBuilder, WorkflowBuilder}
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.exception.TransitionException
import org.antipathy.scoozie.io.ArtefactWriter
import org.antipathy.scoozie.workflow.Workflow

/**
  * Container class for HOCON generated arefacts
  */
final class GeneratedArtefacts(workflow: Workflow,
                               coordinatorOption: Option[Coordinator],
                               validationStringOption: Option[String])
    extends ArtefactWriter {

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
  def saveToPath(outputPath: Path): Unit = {
    this.validate()
    this.writeFile(outputPath.toString, ArtefactWriter.workflowFileName, Scoozie.Formatting.format(this.workflow))
    if (coordinatorOption.isDefined) {
      coordinatorOption.foreach { coordinator =>
        this.writeFile(outputPath.toString, ArtefactWriter.coordinatorFileName, Scoozie.Formatting.format(coordinator))
        this.writeFile(outputPath.toString,
                       ArtefactWriter.propertiesFileName,
                       coordinator.jobProperties + System.lineSeparator() +
                       this.workflow.jobProperties.replace("oozie.wf.application.path", "#oozie.wf.application.path"))
      }
    } else {
      this.writeFile(outputPath.toString, ArtefactWriter.propertiesFileName, this.workflow.jobProperties)
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
