// $COVERAGE-OFF$
package org.antipathy.scoozie.api

import org.antipathy.scoozie.sla._

import scala.collection.immutable.Seq

/**
  * Oozie SLA definitions
  */
object SLA {

  /**
    *
    * @param nominalTime This is the time relative to which your jobs' SLAs will be calculated.
    *                    Generally since Oozie workflows are aligned with synchronous data dependencies,
    *                    this nominal time can be parameterized to be passed the value of your coordinator nominal time.
    *                    Nominal time is also required in case of independent workflows and you can specify the time in
    *                    which you expect the workflow to be run if you don't have a synchronous dataset associated
    *                    with it.
    * @param shouldStart Relative to nominal-time this is the amount of time (along with time-unit - MINUTES, HOURS,
    *                    DAYS) within which your job should start running to meet SLA. This is optional.
    * @param shouldEnd Relative to nominal-time this is the amount of time (along with time-unit - MINUTES, HOURS, DAYS)
    *                    within which your job should finish to meet SLA.
    * @param maxDuration This is the maximum amount of time (along with time-unit - MINUTES, HOURS, DAYS) your job is
    *                    expected to run. This is optional.
    * @param alertEvents Specify the types of events for which Email alerts should be sent. Allowable values in this
    *                    comma-separated list are start_miss, end_miss and duration_miss.
    *                    *_met events can generally be deemed low priority and hence email alerting for these is not
    *                    neccessary. However, note that this setting is only for alerts via email alerts and not via
    *                    JMS messages, where all events send out notifications, and user can filter them using desired
    *                    selectors. This is optional and only applicable when alert-contact is configured.
    * @param alertContacts Specify a comma separated list of email addresses where you wish your alerts to be sent.
    *                     This is optional and need not be configured if you just want to view your job SLA history
    *                     in the UI and do not want to receive email alerts.
    * @param notificationMsg The message to use when an SLA has not been met
    * @param upstreamApps List of upstream applications affected by SLA being missed.
    */
  def create(nominalTime: String,
             shouldStart: Option[String] = None,
             shouldEnd: Option[String] = None,
             maxDuration: Option[String] = None,
             alertEvents: Seq[SLAAlert] = Seq.empty[SLAAlert],
             alertContacts: Seq[String] = Seq.empty[String],
             notificationMsg: Option[String] = None,
             upstreamApps: Seq[String] = Seq.empty[String]): OozieSLA =
    OozieSLA(nominalTime,
             shouldStart,
             shouldEnd,
             maxDuration,
             alertEvents,
             alertContacts,
             notificationMsg,
             upstreamApps)

  /**
    * SLA alert types
    */
  object Alerts {
    val startMiss: SLAAlert = StartMiss
    val endMiss: SLAAlert = EndMiss
    val durationMiss: SLAAlert = DurationMiss
    val all: Seq[SLAAlert] = Seq(StartMiss, EndMiss, DurationMiss)
  }
}
// $COVERAGE-ON$
