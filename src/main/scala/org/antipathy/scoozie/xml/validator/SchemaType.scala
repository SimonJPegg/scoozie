package org.antipathy.scoozie.xml.validator

/**
  * Constants class for defining the types of schemas to validate
  */
private[scoozie] object SchemaType {

  type SchemaType = String

  val workflow: SchemaType = "workflow"
  val coOrdinator: SchemaType = "coordinator"
}
