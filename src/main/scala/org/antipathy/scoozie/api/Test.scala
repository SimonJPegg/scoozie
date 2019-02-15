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
