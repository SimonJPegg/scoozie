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

  protected def buildStringOptionProperty(actionName: String,
                                          propName: String,
                                          option: Option[String]): Map[String, String] = option match {
    case Some(p) => Map(formatProperty(s"${actionName}_$propName") -> p)
    case None    => Map()
  }
}
