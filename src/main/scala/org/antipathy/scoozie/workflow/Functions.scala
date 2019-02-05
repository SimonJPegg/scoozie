package org.antipathy.scoozie.workflow

object Functions {

  /**
    * returns the workflow job ID for the current workflow job.
    */
  val id: String = "${wf:id()}"

  /**
    * returns the workflow application name for the current workflow job.
    */
  val name: String = "${wf:name()}"

  /**
    * returns the workflow application path for the current workflow job.
    */
  val appPath: String = "${wf:appPath()}"

  /**
    * returns the value of the workflow job configuration property for the current workflow job,
    * or an empty string if undefined.
    */
  def conf(name:String): String = "${wf:conf("+name+")}"

  /**
    * returns the user name that started the current workflow job.
    */
  val user: String = "${wf:user()}"

  /**
    * returns the group/ACL for the current workflow job
    */
  val group: String = "${wf:group()}"

  /**
    * returns the callback URL for the current workflow action node, stateVar can be a valid exit
    * state (=OK= or ERROR ) for the action or a token to be replaced with the exit state by the remote
    * system executing the task.
    */
  def callBack(stateVar: String): String = "${wf:callback("+stateVar+")}"

  /**
    * returns the transition taken by the specified workflow action node, or an empty
    * string if the action has not being executed or it has not completed yet.
    */
  def transition(nodeName: String): String = "${wf:transition("+nodeName+")}"

  /**
    * returns the name of the last workflow action node that exit with an ERROR exit state, or an empty string
    * if no a ction has exited with ERROR state in the current workflow job.
    */
  val lastErrorNode: String = "${wf:lastErrorNode()}"

  /**
    * returns the error code for the specified action node, or an empty string
    * if the action node has not exited with ERROR state.
    * Each type of action node must define its complete error code list.
    */
  def errorCode(nodeName:String): String = "${wf:errorCode("+nodeName+")}"

  /**
    * returns the error message for the specified action node, or an empty string if no
    * action node has not exited with ERROR state.
    * The error message can be useful for debugging and notification purposes.
    */
  def errorMessage(nodeName:String): String = "${wf:errorMessage("+nodeName+")}"

  /**
    * returns the run number for the current workflow job, normally 0 unless the workflow
    * job is re-run, in which case indicates the current run.
    */
  val run: String = "${wf:run()}"

  /**
    * This function is only applicable to action nodes that produce output data on completion.
    * The output data is in a Java Properties format and via this EL function it is available as a Map .
    */
  def actionData(nodeName:String): String = "${wf:actionData("+nodeName+")}"

  /**
    * returns the external Id for an action node, or an empty string if the action has
    * not being executed or it has not completed yet.
    */
  def externalActionId(nodeName:String):String = "${wf:actionExternalId("+nodeName+")}"

  /**
    * returns the tracker URIfor an action node, or an empty string if the action has
    * not being executed or it has not completed yet.
    */
  def actionTrackerURL(nodeName:String): String = "${wf:actionTrackerUri("+nodeName+")}"

  /**
    * returns the external status for an action node, or an empty string if the action has not
    * being executed or it has not completed yet.
    */
  def actionExternalStatus(nodeName:String): String = "${wf:actionExternalStatus("+nodeName+")}"
}
