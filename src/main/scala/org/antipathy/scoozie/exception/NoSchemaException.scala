package org.antipathy.scoozie.exception

/**
  * Thrown when unable to find a schema for an oozie artifact
  */
class NoSchemaException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
