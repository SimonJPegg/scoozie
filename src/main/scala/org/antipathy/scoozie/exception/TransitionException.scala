// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

/**
  * Thrown when transition between oozie nodes is impossible
  */
class TransitionException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
