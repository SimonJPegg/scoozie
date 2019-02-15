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
package org.antipathy.scoozie.functions

import org.scalatest.{FlatSpec, Matchers}

class WorkflowFunctionsSpec extends FlatSpec with Matchers {

  behavior of "WorkflowFunctions"

  it should "provide string representations of workflow functions" in {

    val nodeName = "SomeNode"

    WorkflowFunctions.id should be("${wf:id()}")

    WorkflowFunctions.name should be("${wf:name()}")

    WorkflowFunctions.appPath should be("${wf:appPath()}")

    WorkflowFunctions.conf(nodeName) should be("${wf:hocon(" + nodeName + ")}")

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
