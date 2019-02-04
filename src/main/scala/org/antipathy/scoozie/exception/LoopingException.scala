package org.antipathy.scoozie.exception

/**
  * Thrown when a loop is detected in a workfloe
  */
class LoopingException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
