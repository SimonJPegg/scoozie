// $COVERAGE-OFF$
package org.antipathy.scoozie.action.prepare

import org.antipathy.scoozie.XmlSerializable

/**
  * Base trait for workflow preparation Steps
  */
private[scoozie] trait PrepareFSAction extends XmlSerializable {

  /**
    * The path this action operates on
    */
  def path: String
}
// $COVERAGE-ON$
