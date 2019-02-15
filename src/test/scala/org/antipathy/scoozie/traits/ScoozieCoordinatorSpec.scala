package org.antipathy.scoozie.traits

import java.io.{File => JFile}

import better.files._
import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.io.ArtefactWriter
import org.scalatest.{FlatSpec, Matchers}
import org.zeroturnaround.zip.ZipUtil

import scala.collection.immutable.Map

class ScoozieCoordinatorSpec extends FlatSpec with Matchers {

  behavior of "GeneratedCoordinator"

  it should "save generated artifacts" in {
    val testJob = new TestCoordinator("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))

    val outputFolder = File("src/test/resources/output/coordinator")
    testJob.save(outputFolder.path)

    val ouputCoordinator = outputFolder.toString() / ArtefactWriter.coordinatorFileName
    val ouputWorkflow = outputFolder.toString() / ArtefactWriter.workflowFileName
    val ouputProperties = outputFolder.toString() / ArtefactWriter.propertiesFileName

    val workflowText = Scoozie.Formatting.format(testJob.workflow)
    val coordinatorText = Scoozie.Formatting.format(testJob.coordinator)

    ouputCoordinator.lines.mkString(System.lineSeparator()) + System.lineSeparator() should be(coordinatorText)
    ouputWorkflow.lines.mkString(System.lineSeparator()) should be(workflowText)
    ouputProperties.lines.mkString(System.lineSeparator()) + System.lineSeparator() should be(testJob.jobProperties)

    outputFolder.delete()
  }

  it should "save artefacts as a zip file in" in {

    val testJob = new TestCoordinator("yarn", "nameservice1", Map("prop1" -> "value1", "prop2" -> "value2"))

    val outputFolder = File("src/test/resources/output/coordinator/")
    testJob.save(outputFolder.path, asZipFile = true)

    val workflowText = Scoozie.Formatting.format(testJob.workflow)
    val coordinatorText = Scoozie.Formatting.format(testJob.coordinator)
    val propertiesTest = testJob.jobProperties

    val testZip = new JFile(s"${outputFolder.toString()}/${ArtefactWriter.zipArchive}")

    ZipUtil.containsEntry(testZip, ArtefactWriter.coordinatorFileName) should be(true)
    ZipUtil.containsEntry(testZip, ArtefactWriter.workflowFileName) should be(true)
    ZipUtil.containsEntry(testZip, ArtefactWriter.propertiesFileName) should be(true)

    new String(ZipUtil.unpackEntry(testZip, ArtefactWriter.coordinatorFileName)) should be(coordinatorText)
    new String(ZipUtil.unpackEntry(testZip, ArtefactWriter.workflowFileName)) should be(workflowText)
    new String(ZipUtil.unpackEntry(testZip, ArtefactWriter.propertiesFileName)) should be(propertiesTest)

  }
}
