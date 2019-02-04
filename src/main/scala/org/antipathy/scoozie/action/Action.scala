package org.antipathy.scoozie.action

import org.antipathy.scoozie.{Nameable, OozieProperties, XmlSerializable}

/**
  * Base trait for oozie actions
  */
private[scoozie] trait Action extends XmlSerializable with Nameable with OozieProperties {

  /**
    * The XML namespace for an action element
    */
  def xmlns: Option[String]
}
