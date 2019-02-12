// $COVERAGE-OFF$
package org.antipathy.scoozie.sla

import org.antipathy.scoozie.action.Nameable

/**
  * base trait for Oozie SLAs
  */
sealed trait SLAAlert extends Nameable

case object StartMiss extends SLAAlert { override val name: String = "start_miss" }
case object EndMiss extends SLAAlert { override val name: String = "end_miss" }
case object DurationMiss extends SLAAlert { override val name: String = "duration_miss" }
// $COVERAGE-ON$
