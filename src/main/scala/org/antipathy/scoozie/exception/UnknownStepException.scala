// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import org.antipathy.scoozie.Scoozie

/**
  * Thrown when an unexpected prepare step is encountered
  */
class UnknownStepException(message: String, cause: Throwable = Scoozie.Null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
