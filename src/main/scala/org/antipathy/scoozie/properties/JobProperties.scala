// $COVERAGE-OFF$
package org.antipathy.scoozie.properties

/**
  * Base trait for obtaining job properties
  */
private[scoozie] trait JobProperties {

  /**
    * Get the job properties
    */
  def jobProperties: String
}
// $COVERAGE-ON$
