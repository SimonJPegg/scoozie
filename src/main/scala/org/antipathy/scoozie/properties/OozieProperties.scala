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
package org.antipathy.scoozie.properties

import scala.collection.immutable.{Map, Seq}

/**
  * base trait for getting Oozie properties
  */
private[scoozie] trait OozieProperties extends PropertyFormatter {

  /**
    * Get the Oozie properties for this object
    */
  def properties: Map[String, String]

  /**
    * Construct a map of property values to their substituted names
    * @param propertyName the name of the substituted property
    * @param propertyValue the value of the property
    * @return A map of the value to the new name
    */
  def buildStringProperty(propertyName: String, propertyValue: String): Map[String, String] =
    if (propertyValue.isEmpty) {
      Map[String, String]()
    } else {
      Map(formatProperty(propertyName) -> propertyValue)
    }

  /**
    * Convert a sequence of property values to a map of KV pairs
    * @param actionName The name of the action to insert into the property name
    * @param valueSequence The property values
    */
  protected def buildSequenceProperties(actionName: String,
                                        propName: String,
                                        valueSequence: Seq[String]): Map[String, String] =
    valueSequence.zipWithIndex.flatMap {
      case (param, index) =>
        Map(formatProperty(s"${actionName}_$propName$index") -> param)
    }.toMap

  /**
    * Convert an optional property to a map of KV pairs
    * @param actionName The name of the action to insert into the property name
    * @param propName The name of the property
    * @param option the optional value to convert
    */
  protected def buildStringOptionProperty(actionName: String,
                                          propName: String,
                                          option: Option[String]): Map[String, String] = option match {
    case Some(p) => Map(formatProperty(s"${actionName}_$propName") -> p)
    case None    => Map()
  }

  /**
    *  Convert a sequence of properties to a single (comma separated) value  and map to a KV pair
    * @param actionName The name of the action to insert into the property name
    * @param propName The name of the property
    * @param valueSequence The property values
    */
  protected def buildSequenceToSingleValueProperty(actionName: String,
                                                   propName: String,
                                                   valueSequence: Seq[String]): Map[String, String] =
    valueSequence match {
      case Nil => Map()
      case values =>
        val singleVal = values.mkString(",")
        Map(formatProperty(s"${actionName}_$propName") -> singleVal)
    }
}
