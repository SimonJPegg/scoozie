// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import com.typesafe.config.ConfigException

/**
  * Thrown when a configuration item cannot be found
  */
class ConfigurationMissingException(message: String, cause: Throwable = null) extends ConfigException(message, cause)
// $COVERAGE-ON$
