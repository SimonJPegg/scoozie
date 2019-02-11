package org.antipathy.scoozie.builder

import com.typesafe.config.Config

/**
  * Class for building transition String for worflow validation
  */
object TransitionStringBuilder {

  /**
    * Get the transition string for this workflow
    */
  def build(config: Config): String =
    config.getString(s"${HoconConstants.validate}.${HoconConstants.transitions}").replace("  ", " ")
}
