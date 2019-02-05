package org.antipathy.scoozie.functions

import org.scalatest.{FlatSpec, Matchers}

class WorkflowFunctionsSpec extends FlatSpec with Matchers {

  behavior of "WorkflowFunctions"

  it should "provide string representations of workflow functions" in {

    val nodeName = "SomeNode"

    WorkflowFunctions.id should be("${wf:id()}")

    WorkflowFunctions.name should be("${wf:name()}")

    WorkflowFunctions.appPath should be("${wf:appPath()}")

    WorkflowFunctions.conf(nodeName) should be("${wf:conf(" + nodeName + ")}")

    WorkflowFunctions.user should be("${wf:user()}")

    WorkflowFunctions.group should be("${wf:group()}")

    WorkflowFunctions.callBack(nodeName) should be("${wf:callback(" + nodeName + ")}")

    WorkflowFunctions.transition(nodeName) should be("${wf:transition(" + nodeName + ")}")

    WorkflowFunctions.lastErrorNode should be("${wf:lastErrorNode()}")

    WorkflowFunctions.errorCode(nodeName) should be("${wf:errorCode(" + nodeName + ")}")

    WorkflowFunctions.errorMessage(nodeName) should be("${wf:errorMessage(" + nodeName + ")}")

    WorkflowFunctions.run should be("${wf:run()}")

    WorkflowFunctions.actionData(nodeName) should be("${wf:actionData(" + nodeName + ")}")

    WorkflowFunctions.externalActionId(nodeName) should be("${wf:actionExternalId(" + nodeName + ")}")

    WorkflowFunctions.actionTrackerURL(nodeName) should be("${wf:actionTrackerUri(" + nodeName + ")}")

    WorkflowFunctions.actionExternalStatus(nodeName) should be("${wf:actionExternalStatus(" + nodeName + ")}")
  }
}
