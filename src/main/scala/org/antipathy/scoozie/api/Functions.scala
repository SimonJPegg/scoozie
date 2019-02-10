package org.antipathy.scoozie.api

import org.antipathy.scoozie.functions.{BasicFunctions, CoordinatorFunctions, WorkflowFunctions}

/**
  * Oozie EL functions
  */
object Functions {

  /**
    * Oozie workflow functions
    */
  object WorkFlow {

    /**
      * returns the workflow job ID for the current workflow job.
      */
    val id: String = WorkflowFunctions.id

    /**
      * returns the workflow application name for the current workflow job.
      */
    val name: String = WorkflowFunctions.name

    /**
      * returns the workflow application path for the current workflow job.
      */
    val appPath: String = WorkflowFunctions.appPath

    /**
      * returns the value of the workflow job configuration property for the current workflow job,
      * or an empty string if undefined.
      */
    def conf(name: String): String = WorkflowFunctions.conf(name)

    /**
      * returns the user name that started the current workflow job.
      */
    val user: String = WorkflowFunctions.user

    /**
      * returns the group/ACL for the current workflow job
      */
    val group: String = WorkflowFunctions.group

    /**
      * returns the callback URL for the current workflow action node, stateVar can be a valid exit
      * state (=OK= or ERROR ) for the action or a token to be replaced with the exit state by the remote
      * system executing the task.
      */
    def callBack(stateVar: String): String = WorkflowFunctions.callBack(stateVar)

    /**
      * returns the transition taken by the specified workflow action node, or an empty
      * string if the action has not being executed or it has not completed yet.
      */
    def transition(nodeName: String): String = WorkflowFunctions.transition(nodeName)

    /**
      * returns the name of the last workflow action node that exit with an ERROR exit state, or an empty string
      * if no a ction has exited with ERROR state in the current workflow job.
      */
    val lastErrorNode: String = WorkflowFunctions.lastErrorNode

    /**
      * returns the error code for the specified action node, or an empty string
      * if the action node has not exited with ERROR state.
      * Each type of action node must define its complete error code list.
      */
    def errorCode(nodeName: String): String = WorkflowFunctions.errorCode(nodeName)

    /**
      * returns the error message for the specified action node, or an empty string if no
      * action node has not exited with ERROR state.
      * The error message can be useful for debugging and notification purposes.
      */
    def errorMessage(nodeName: String): String = WorkflowFunctions.errorMessage(nodeName)

    /**
      * returns the run number for the current workflow job, normally 0 unless the workflow
      * job is re-run, in which case indicates the current run.
      */
    val run: String = WorkflowFunctions.run

    /**
      * This function is only applicable to action nodes that produce output data on completion.
      * The output data is in a Java Properties format and via this EL function it is available as a Map .
      */
    def actionData(nodeName: String): String = WorkflowFunctions.actionData(nodeName)

    /**
      * returns the external Id for an action node, or an empty string if the action has
      * not being executed or it has not completed yet.
      */
    def externalActionId(nodeName: String): String = WorkflowFunctions.externalActionId(nodeName)

    /**
      * returns the tracker URIfor an action node, or an empty string if the action has
      * not being executed or it has not completed yet.
      */
    def actionTrackerURL(nodeName: String): String = WorkflowFunctions.actionTrackerURL(nodeName)

    /**
      * returns the external status for an action node, or an empty string if the action has not
      * being executed or it has not completed yet.
      */
    def actionExternalStatus(nodeName: String): String = WorkflowFunctions.actionExternalStatus(nodeName)
  }

  /**
    * Oozie basic functions
    */
  object Basic {

    /**
      * returns the first not null value, or null if both are null .Note that if the output of this function is null and
      * it is used as string, the EL library converts it to an empty string. This is the common behavior when using
      * firstNotNull() in node configuration sections.
      */
    def firstNotNull(value1: String, value2: String): String = BasicFunctions.firstNotNull(value1, value2)

    /**
      * returns the concatenation of 2 strings. A string with null value is considered as an empty string.
      */
    def concat(s1: String, s2: String): String = BasicFunctions.concat(s1, s2)

    /**
      * Replace each occurrence of regular expression match in the first string with the replacement
      * string and return the replaced string. A 'regex' string with null value is considered as no change.
      * A 'replacement' string with null value is consider as an empty string.
      */
    def replaceAll(src: String, regex: String, replacement: String): String =
      BasicFunctions.replaceAll(src, regex, replacement)

    /**
      * Add the append string into each splitted sub-strings of the first string(=src=). The split is performed
      * into src string using the delimiter . E.g. appendAll("/a/b/,/c/b/,/c/d/", "ADD", ",") will
      * return /a/b/ADD,/c/b/ADD,/c/d/ADD . A append string with null value is consider as an empty string.
      * A delimiter string with value null is considered as no append in the string.
      */
    def appendAll(src: String, append: String, delimeter: String): String =
      BasicFunctions.appendAll(src, append, delimeter)

    /**
      * returns the trimmed value of the given string. A string with null value is considered as an empty string.
      */
    def trim(s: String): String = BasicFunctions.trim(s)

    /**
      * returns the URL UTF-8 encoded value of the given string. A string with null
      * value is considered as an empty string.
      */
    def urlEncode(s: String): String = BasicFunctions.urlEncode(s)

    /**
      * returns the UTC current date and time in W3C format down to the
      * second (YYYY-MM-DDThh:mm:ss.sZ). I.e.: 1997-07-16T19:20:30.45Z
      */
    val timestamp: String = BasicFunctions.timestamp

    /**
      * returns an XML encoded JSON representation of a Map. This function is useful to encode as a single
      * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it
      * in full to another action.
      */
    def toJsonStr(variable: String): String = BasicFunctions.toJsonStr(variable)

    /**
      * returns an XML encoded Properties representation of a Map. This function is useful to encode as a single
      * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it in
      * full to another action.
      */
    def toPropertiesStr(variable: String): String = BasicFunctions.toPropertiesStr(variable)

    /**
      * returns an XML encoded Configuration representation of a Map. This function is useful to encode as a single
      * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it in full
      * to another action.
      */
    def toConfigurationStr(variable: String): String = BasicFunctions.toConfigurationStr(variable)
  }

  /**
    * oozie coordinator functions
    */
  object Coordinator {

    /**
      * Oozie time frequency in minutes
      */
    def minutes(i: Int): String = CoordinatorFunctions.minutes(i)

    /**
      * Oozie time frequency in hours
      */
    def hours(i: Int): String = CoordinatorFunctions.hours(i)

    /**
      * Oozie time frequency in days
      */
    def days(i: Int): String = CoordinatorFunctions.days(i)

    /**
      * Oozie time frequency in days
      * identical to the `days` function except that it shifts the first occurrence to the end of the day
      * for the specified timezone before computing the interval in minutes
      */
    def endOfDays(i: Int): String = CoordinatorFunctions.endOfDays(i)

    /**
      * Oozie time frequency in months
      */
    def months(i: Int): String = CoordinatorFunctions.months(i)

    /**
      * Oozie time frequency in months
      * identical to the `months` function except that it shifts the first occurrence to the end of the month
      * for the specified timezone before computing the interval in minutes
      */
    def endOfMonths(i: Int): String = CoordinatorFunctions.endOfMonths(i)

    /**
      * Oozie time frequency in cron format
      */
    def cron(string: String): String = CoordinatorFunctions.cron(string)
  }
}
