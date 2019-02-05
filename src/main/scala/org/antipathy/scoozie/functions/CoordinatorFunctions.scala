package org.antipathy.scoozie.functions

/**
  * oozie coordinator functions
  */
object CoordinatorFunctions {

  /**
    * Oozie time frequency in minutes
    */
  def minutes(i: Int): String = "${coord:minutes(" + i + ")}"

  /**
    * Oozie time frequency in hours
    */
  def hours(i: Int): String = "${coord:hours(" + i + ")}"

  /**
    * Oozie time frequency in days
    */
  def days(i: Int): String = "${coord:days(" + i + ")}"

  /**
    * Oozie time frequency in days
    * identical to the `days` function except that it shifts the first occurrence to the end of the day
    * for the specified timezone before computing the interval in minutes
    */
  def endOfDays(i: Int): String = "${coord:coord:endOfDays(" + i + ")}"

  /**
    * Oozie time frequency in months
    */
  def months(i: Int): String = "${coord:months(" + i + ")}"

  /**
    * Oozie time frequency in months
    * identical to the `months` function except that it shifts the first occurrence to the end of the month
    * for the specified timezone before computing the interval in minutes
    */
  def endOfMonths(i: Int): String = "${coord:endOfMonths(" + i + ")}"

  /**
    * Oozie time frequency in cron format
    */
  def cron(string: String): String = "${" + string + "}"
}
