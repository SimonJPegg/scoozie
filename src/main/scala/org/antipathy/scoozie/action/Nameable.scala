package org.antipathy.scoozie.action

/**
  * base trait for namable objects
  */
private[scoozie] trait Nameable {

  /**
    * The name of the object
    */
  def name: String
}
