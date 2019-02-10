package org.antipathy.scoozie

import org.scalatest.{FlatSpec, Matchers}
import better.files._
import org.antipathy.scoozie.io.ArtefactWriter

import scala.collection.immutable.Map

class ScoozieWorkflowSpec extends FlatSpec with Matchers {

  behavior of "GeneratedWorkflow"

  it should "save generated artifacts" in {
    val testJob = new TestJob("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))

    val outputFolder = File("src/test/resources/output/workflow")
    testJob.saveWorkflow(outputFolder.toString())

    val ouputWorkflow = outputFolder.toString() / ArtefactWriter.workflowFileName
    val ouputProperties = outputFolder.toString() / ArtefactWriter.propertiesFileName

    val workflowText = Scoozie.Formatting.format(testJob.workflow)

    ouputWorkflow.lines.mkString(System.lineSeparator()) should be(workflowText)
    ouputProperties.lines.mkString(System.lineSeparator()) should be(testJob.jobProperties)

    outputFolder.delete()
  }

}
