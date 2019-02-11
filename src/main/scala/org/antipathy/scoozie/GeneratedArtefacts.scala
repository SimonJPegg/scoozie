package org.antipathy.scoozie

import com.typesafe.config.Config
import org.antipathy.scoozie.builder.CoordinatorBuilder
import org.antipathy.scoozie.builder.WorkflowBuilder
import org.antipathy.scoozie.builder.TransitionStringBuilder
import org.antipathy.scoozie.workflow.Workflow
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.exception.TransitionException
import java.nio.file.Path

import org.antipathy.scoozie.io.ArtefactWriter

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
    if (validationStringOption.isDefined) {
      val validationString = validationStringOption.get
      val traversalString = Scoozie.Test.workflowTesterWorkflowTestRunner(workflow).traversalPath

      if (!validationString.equals(traversalString)) {
        throw new TransitionException(s"""Expected transition:
                                         |$validationString
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
                       coordinator.jobProperties + System.lineSeparator() + this.workflow.jobProperties)
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
