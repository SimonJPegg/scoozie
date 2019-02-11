// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import com.typesafe.config.ConfigException
import org.antipathy.scoozie.Scoozie

/**
  * Thrown when a configuration item cannot be found
  */
class ConfigurationMissingException(message: String, cause: Throwable = Scoozie.Null)
    extends ConfigException(message, cause)
// $COVERAGE-ON$
