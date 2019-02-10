// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

/**
  * Thrown when an unexpected prepare step is encountered
  */
class UnknownStepException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
