package org.antipathy.scoozie

import org.scalatest.{FlatSpec, Matchers}
import scala.collection.immutable.Map
import org.antipathy.scoozie.io.ArtefactWriter
import better.files._

class GeneratedCoordinatorSpec extends FlatSpec with Matchers {

  behavior of "GeneratedCoordinator"

  it should "save generated artifacts" in {
    val testJob = new TestJob("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))

    val outputFolder = File("src/test/resources/output/coordinator")
    testJob.saveCoordinator(outputFolder.toString())

    val ouputCoordinator = outputFolder.toString() / ArtefactWriter.coordinatorFileName
    val ouputWorkflow = outputFolder.toString() / ArtefactWriter.workflowFileName
    val ouputProperties = outputFolder.toString() / ArtefactWriter.propertiesFileName

    val workflowText = Scoozie.Formatting.format(testJob.workflow)
    val coordinatorText = Scoozie.Formatting.format(testJob.coordinator)

    ouputCoordinator.lines.mkString(System.lineSeparator()) + System.lineSeparator() should be(coordinatorText)
    ouputWorkflow.lines.mkString(System.lineSeparator()) should be(workflowText)
    ouputProperties.lines.mkString(System.lineSeparator()) should be(testJob.jobProperties)

    outputFolder.delete()
  }
}
