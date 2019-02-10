package org.antipathy.scoozie.api
import org.antipathy.scoozie.Scoozie.{Formatting => TestFormatting}
import org.antipathy.scoozie.coordinator.Coordinator
import org.antipathy.scoozie.testing.WorkflowTestRunner
import org.antipathy.scoozie.workflow.Workflow
import org.antipathy.scoozie.xml.validator.{OozieValidator, SchemaType}

import scala.collection.immutable.Seq

/**
  * Methods for testing Oozie workflows
  */
object Test {

  /**
    * Wrap the passed in workflow in a test runner
    * @param workflow the workflow to test
    * @param failingNodes a list of nodes that should fail in this workflow
    * @param decisionNodes the nodes to visit on a decision
    */
  def workflowTesterWorkflowTestRunner(workflow: Workflow,
                                       failingNodes: Seq[String] = Seq.empty[String],
                                       decisionNodes: Seq[String] = Seq.empty[String]): WorkflowTestRunner = {
    validate(workflow)
    new WorkflowTestRunner(workflow, failingNodes, decisionNodes)
  }

  /**
    * Validate the passed in workflow
    * @param workflow the workflow to validate
    */
  def validate(workflow: Workflow): Unit =
    OozieValidator.validate(TestFormatting.format(workflow), SchemaType.workflow)

  /**
    * Validate the passed in coordinator
    * @param coOrdinator the coordinator to validate
    */
  def validate(coOrdinator: Coordinator): Unit =
    OozieValidator.validate(TestFormatting.format(coOrdinator), SchemaType.coOrdinator)
}
