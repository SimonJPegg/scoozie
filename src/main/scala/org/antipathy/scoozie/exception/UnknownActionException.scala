// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import org.antipathy.scoozie.Scoozie

/**
  * Thrown when an unexpected action is encountered
  */
class UnknownActionException(message: String, cause: Throwable = Scoozie.Null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
