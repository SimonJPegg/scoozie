package org.antipathy.scoozie.functions

/**
  * Oozie workflow functions
  */
object WorkflowFunctions {

  /**
    * returns the workflow job ID for the current workflow job.
    */
  val id: String = variablePrefix + "wf:id(" + variablePostFix

  /**
    * returns the workflow application name for the current workflow job.
    */
  val name: String = variablePrefix + "wf:name( " + variablePostFix

  /**
    * returns the workflow application path for the current workflow job.
    */
  val appPath: String = variablePrefix + "wf:appPath(" + variablePostFix

  /**
    * returns the value of the workflow job configuration property for the current workflow job,
    * or an empty string if undefined.
    */
  def conf(name: String): String = variablePrefix + "wf:hocon(" + name + variablePostFix

  /**
    * returns the user name that started the current workflow job.
    */
  val user: String = variablePrefix + "wf:user()}"

  /**
    * returns the group/ACL for the current workflow job
    */
  val group: String = variablePrefix + "wf:group()}"

  /**
    * returns the callback URL for the current workflow action node, stateVar can be a valid exit
    * state (=OK= or ERROR ) for the action or a token to be replaced with the exit state by the remote
    * system executing the task.
    */
  def callBack(stateVar: String): String = variablePrefix + "wf:callback(" + stateVar + variablePostFix

  /**
    * returns the transition taken by the specified workflow action node, or an empty
    * string if the action has not being executed or it has not completed yet.
    */
  def transition(nodeName: String): String = variablePrefix + "wf:transition(" + nodeName + variablePostFix

  /**
    * returns the name of the last workflow action node that exit with an ERROR exit state, or an empty string
    * if no a ction has exited with ERROR state in the current workflow job.
    */
  val lastErrorNode: String = variablePrefix + "wf:lastErrorNode()}"

  /**
    * returns the error code for the specified action node, or an empty string
    * if the action node has not exited with ERROR state.
    * Each type of action node must define its complete error code list.
    */
  def errorCode(nodeName: String): String = variablePrefix + "wf:errorCode(" + nodeName + variablePostFix

  /**
    * returns the error message for the specified action node, or an empty string if no
    * action node has not exited with ERROR state.
    * The error message can be useful for debugging and notification purposes.
    */
  def errorMessage(nodeName: String): String = variablePrefix + "wf:errorMessage(" + nodeName + variablePostFix

  /**
    * returns the run number for the current workflow job, normally 0 unless the workflow
    * job is re-run, in which case indicates the current run.
    */
  val run: String = variablePrefix + "wf:run()}"

  /**
    * This function is only applicable to action nodes that produce output data on completion.
    * The output data is in a Java Properties format and via this EL function it is available as a Map .
    */
  def actionData(nodeName: String): String = variablePrefix + "wf:actionData(" + nodeName + variablePostFix

  /**
    * returns the external Id for an action node, or an empty string if the action has
    * not being executed or it has not completed yet.
    */
  def externalActionId(nodeName: String): String = variablePrefix + "wf:actionExternalId(" + nodeName + variablePostFix

  /**
    * returns the tracker URIfor an action node, or an empty string if the action has
    * not being executed or it has not completed yet.
    */
  def actionTrackerURL(nodeName: String): String = variablePrefix + "wf:actionTrackerUri(" + nodeName + variablePostFix

  /**
    * returns the external status for an action node, or an empty string if the action has not
    * being executed or it has not completed yet.
    */
  def actionExternalStatus(nodeName: String): String = variablePrefix + "wf:actionExternalStatus(" + nodeName + variablePostFix

  private val variablePrefix: String = "${"
  private val variablePostFix: String = ")}"
}
