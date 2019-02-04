package org.antipathy.scoozie

import scala.collection.immutable._

/**
  * base trait for getting Oozie properties
  */
private[scoozie] trait OozieProperties {

  /**
    * Get the Oozie properties for this object
    */
  def properties: Map[String, String]

  /**
    * expected predicate pattern for oozie switches
    */
  private val Pattern = """[${].*[}]""".r

  /**
    * format predicates to expected pattern
    */
  protected def formatProperty(property: String): String = property match {
    case Pattern() => property
    case _         => "${" + property + "}"
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

  protected def buildStringOptionProperty(actionName: String,
                                          propName: String,
                                          option: Option[String]): Map[_ <: String, String] = option match {
    case Some(p) => Map(formatProperty(s"${actionName}_$propName") -> p)
    case None    => Map()
  }
}
