// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import org.antipathy.scoozie.Scoozie

/**
  * Thrown when transition between oozie nodes is impossible
  */
class TransitionException(message: String, cause: Throwable = Scoozie.Null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
