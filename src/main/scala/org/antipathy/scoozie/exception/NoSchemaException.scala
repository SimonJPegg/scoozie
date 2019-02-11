// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import org.antipathy.scoozie.Scoozie

/**
  * Thrown when unable to find a schema for an oozie artifact
  */
class NoSchemaException(message: String, cause: Throwable = Scoozie.Null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
