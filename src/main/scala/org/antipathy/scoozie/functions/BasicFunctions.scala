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
  * Oozie basic functions
  */
object BasicFunctions {

  /**
    * returns the first not null value, or null if both are null .Note that if the output of this function is null and
    * it is used as string, the EL library converts it to an empty string. This is the common behavior when using
    * firstNotNull() in node configuration sections.
    */
  def firstNotNull(value1: String, value2: String): String = s"firstNotNull($value1,$value2)"

  /**
    * returns the concatenation of 2 strings. A string with null value is considered as an empty string.
    */
  def concat(s1: String, s2: String): String = s"concat($s1,$s2)"

  /**
    * Replace each occurrence of regular expression match in the first string with the replacement
    * string and return the replaced string. A 'regex' string with null value is considered as no change.
    * A 'replacement' string with null value is consider as an empty string.
    */
  def replaceAll(src: String, regex: String, replacement: String): String = s"replaceAll($src,$regex,$replacement)"

  /**
    * Add the append string into each splitted sub-strings of the first string(=src=). The split is performed
    * into src string using the delimiter . E.g. appendAll("/a/b/,/c/b/,/c/d/", "ADD", ",") will
    * return /a/b/ADD,/c/b/ADD,/c/d/ADD . A append string with null value is consider as an empty string.
    * A delimiter string with value null is considered as no append in the string.
    */
  def appendAll(src: String, append: String, delimeter: String): String = s"appendAll($src,$append,$delimeter)"

  /**
    * returns the trimmed value of the given string. A string with null value is considered as an empty string.
    */
  def trim(s: String): String = s"trim($s)"

  /**
    * returns the URL UTF-8 encoded value of the given string. A string with null
    * value is considered as an empty string.
    */
  def urlEncode(s: String): String = s"urlEncode($s)"

  /**
    * returns the UTC current date and time in W3C format down to the
    * second (YYYY-MM-DDThh:mm:ss.sZ). I.e.: 1997-07-16T19:20:30.45Z
    */
  val timestamp: String = "timestamp()"

  /**
    * returns an XML encoded JSON representation of a Map. This function is useful to encode as a single
    * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it
    * in full to another action.
    */
  def toJsonStr(variable: String): String = s"toJsonStr($variable)"

  /**
    * returns an XML encoded Properties representation of a Map. This function is useful to encode as a single
    * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it in
    * full to another action.
    */
  def toPropertiesStr(variable: String): String = s"toPropertiesStr($variable)"

  /**
    * returns an XML encoded Configuration representation of a Map. This function is useful to encode as a single
    * property the complete action-data of an action, wf:actionData(String actionName) , in order to pass it in full
    * to another action.
    */
  def toConfigurationStr(variable: String): String = s"toConfigurationStr($variable)"

}
