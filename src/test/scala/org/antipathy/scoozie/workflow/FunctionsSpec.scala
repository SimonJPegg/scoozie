package org.antipathy.scoozie.workflow
import org.scalatest.{FlatSpec, Matchers}

class FunctionsSpec extends FlatSpec with Matchers {

  behavior of "Functions"

  it should "provide string representations of workflow fuctions" in {
    
    val nodeName = "SomeNode"

    Functions.id should be( "${wf:id()}")

    Functions.name should be( "${wf:name()}")

    Functions.appPath should be( "${wf:appPath()}")

    Functions.conf(nodeName) should be( "${wf:conf("+nodeName+")}")

    Functions.user should be( "${wf:user()}")

    Functions.group should be( "${wf:group()}")

    Functions.callBack(nodeName) should be( "${wf:callback("+nodeName+")}")

    Functions.transition(nodeName) should be( "${wf:transition("+nodeName+")}")

    Functions.lastErrorNode should be( "${wf:lastErrorNode()}")

    Functions.errorCode(nodeName) should be( "${wf:errorCode("+nodeName+")}")

    Functions.errorMessage(nodeName) should be( "${wf:errorMessage("+nodeName+")}")

    Functions.run should be( "${wf:run()}")

    Functions.actionData(nodeName) should be( "${wf:actionData("+nodeName+")}")

    Functions.externalActionId(nodeName) should be( "${wf:actionExternalId("+nodeName+")}")

    Functions.actionTrackerURL(nodeName) should be( "${wf:actionTrackerUri("+nodeName+")}")

    Functions.actionExternalStatus(nodeName) should be( "${wf:actionExternalStatus("+nodeName+")}")
  }
}
