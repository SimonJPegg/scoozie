// $COVERAGE-OFF$
package org.antipathy.scoozie.sla

import org.antipathy.scoozie.action.Nameable
import org.antipathy.scoozie.exception.InvalidConfigurationException

/**
  * base trait for Oozie SLAs
  */
sealed trait SLAAlert extends Nameable

case object StartMiss extends SLAAlert { override val name: String = SLAAlert.startMiss }
case object EndMiss extends SLAAlert { override val name: String = SLAAlert.endMiss }
case object DurationMiss extends SLAAlert { override val name: String = SLAAlert.durationMiss }
// $COVERAGE-ON$

object SLAAlert {

  private[sla] val startMiss = "start_miss"
  private[sla] val endMiss = "end_miss"
  private[sla] val durationMiss = "duration_miss"

  def fromName(name: String): SLAAlert = name match {
    case SLAAlert.startMiss    => StartMiss
    case SLAAlert.endMiss      => EndMiss
    case SLAAlert.durationMiss => DurationMiss
    case unknown               => throw new InvalidConfigurationException(s"$unknown is not a valid SLA alert type")
  }
}
