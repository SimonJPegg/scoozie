// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

/**
  * Thrown when an unexpected action is encountered
  */
class UnknownActionException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
