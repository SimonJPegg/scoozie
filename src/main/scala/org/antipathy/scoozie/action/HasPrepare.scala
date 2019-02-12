// $COVERAGE-OFF$
package org.antipathy.scoozie.action
import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.ActionProperties

import scala.collection.immutable.Map
import scala.xml.Elem

/**
  * Trait for building prepare properties
  */
private[scoozie] trait HasPrepare {
  this: Nameable =>


  def prepareOption: Option[Prepare]

  //map the prepare step for this action
  protected val prepareOptionAndProps: Option[ActionProperties[Prepare]] =
    prepareOption.map(_.withActionProperties(name))
  protected val prepareProperties: Map[String, String] =
    prepareOptionAndProps.map(_.properties).getOrElse(Map[String, String]())
  protected val prepareOptionMapped: Option[Prepare] = prepareOptionAndProps.map(_.mappedType)

  /**
    * Render the XML for this prepare step
    */
  protected def prepareXML: Elem = prepareOptionMapped.map(_.toXML).orNull
}
// $COVERAGE-ON$
