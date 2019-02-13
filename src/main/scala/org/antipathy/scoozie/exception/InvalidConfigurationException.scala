// $COVERAGE-OFF$
package org.antipathy.scoozie.exception

import com.typesafe.config.ConfigException
import org.antipathy.scoozie.Scoozie

/**
  * Thrown when a configuration item is not valid
  */
class InvalidConfigurationException(message: String, cause: Throwable = Scoozie.Null)
    extends ConfigException(message, cause)
// $COVERAGE-ON$
