// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import org.antipathy.scoozie.Scoozie

/**
  * Thrown when a loop is detected in a workfloe
  */
class LoopingException(message: String, cause: Throwable = Scoozie.Null) extends RuntimeException(message, cause)
// $COVERAGE-ON$
