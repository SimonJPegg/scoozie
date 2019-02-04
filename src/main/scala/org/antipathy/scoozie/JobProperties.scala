package org.antipathy.scoozie

/**
  * Base trait for obtaining job properties
  */
private[scoozie] trait JobProperties {

  /**
    * Get the job properties
    */
  def jobProperties: String
}
