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

/**
  * oozie coordinator functions
  */
object CoordinatorFunctions {

  /**
    * Oozie time frequency in minutes
    */
  def minutes(i: Int): String = variablePrefix + "coord:minutes(" + i + variablePostFix

  /**
    * Oozie time frequency in hours
    */
  def hours(i: Int): String = variablePrefix + "coord:hours(" + i + variablePostFix

  /**
    * Oozie time frequency in days
    */
  def days(i: Int): String = variablePrefix + "coord:days(" + i + variablePostFix

  /**
    * Oozie time frequency in days
    * identical to the `days` function except that it shifts the first occurrence to the end of the day
    * for the specified timezone before computing the interval in minutes
    */
  def endOfDays(i: Int): String = variablePrefix + "coord:coord:endOfDays(" + i + variablePostFix

  /**
    * Oozie time frequency in months
    */
  def months(i: Int): String = variablePrefix + "coord:months(" + i + variablePostFix

  /**
    * Oozie time frequency in months
    * identical to the `months` function except that it shifts the first occurrence to the end of the month
    * for the specified timezone before computing the interval in minutes
    */
  def endOfMonths(i: Int): String = variablePrefix + "coord:endOfMonths(" + i + variablePostFix

  /**
    * Oozie time frequency in cron format
    */
  def cron(string: String): String = variablePrefix + string + "}"

  private val variablePrefix: String = "${"
  private val variablePostFix: String = ")}"

}
