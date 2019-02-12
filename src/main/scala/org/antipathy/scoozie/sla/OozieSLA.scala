package org.antipathy.scoozie.sla

import org.antipathy.scoozie.action.Nameable
import org.antipathy.scoozie.properties.OozieProperties
import org.antipathy.scoozie.xml.XmlSerializable
import org.antipathy.scoozie.configuration.ActionProperties
import scala.collection.immutable._
import scala.xml.Elem

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
case class OozieSLA(nominalTime: String,
                    shouldStart: Option[String],
                    shouldEnd: Option[String],
                    maxDuration: Option[String],
                    alertEvents: Seq[SLAAlert],
                    alertContacts: Seq[String],
                    notificationMsg: Option[String],
                    upstreamApps: Seq[String],
                    override val name: String = "_sla")
    extends XmlSerializable
    with OozieProperties
    with Nameable {

  private val nominalTimeProperty = buildStringOptionProperty(name, "nominalTime", Some(nominalTime))
  private val shouldStartProperty = buildStringOptionProperty(name, "shouldStart", shouldStart)
  private val shouldEndProperty = buildStringOptionProperty(name, "shouldEnd", shouldEnd)
  private val maxDurationProperty = buildStringOptionProperty(name, "maxDuration", maxDuration)
  private val alertEventsProperty =
    buildStringOptionProperty(name, s"alertEvents", Some(alertEvents.map(_.name).mkString(",")))
  private val alertContactsProperty =
    buildStringOptionProperty(name, s"alertContacts", Some(alertContacts.mkString(",")))
  private val notificationMsgProperty = buildStringOptionProperty(name, s"notificationMsg", notificationMsg)
  private val upstreamAppsProperty =
    buildStringOptionProperty(name, s"upstreamApps", Some(upstreamApps.mkString(",")))

  /**
    * Add the owning node name to this SLA object
    */
  def withActionName(actionName: String): ActionProperties[OozieSLA] =
    ActionProperties(this.copy(name = s"$actionName${this.name}"), this.properties)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    nominalTimeProperty ++
    shouldStartProperty ++
    shouldEndProperty ++
    maxDurationProperty ++
    alertEventsProperty ++
    alertContactsProperty ++
    notificationMsgProperty ++
    upstreamAppsProperty

  /**
    * The XML for this node
    */
  override def toXML: Elem = <sla:info>
    {nominalTimeProperty.keys.map(k => <sla:nominal-time>{formatProperty(k)}</sla:nominal-time>)}
    {shouldStartProperty.keys.map(k => <sla:should-start>{formatProperty(k)}</sla:should-start>)}
    {shouldStartProperty.keys.map(k => <sla:should-end>{formatProperty(k)}</sla:should-end>)}
    {maxDurationProperty.keys.map(k => <sla:max-duration>{formatProperty(k)}</sla:max-duration>)}
    {alertEventsProperty.keys.map(k => <sla:alert-events>{formatProperty(k)}</sla:alert-events>)}
    {alertContactsProperty.keys.map(k => <sla:alert-contact>{formatProperty(k)}</sla:alert-contact>)}
    {notificationMsgProperty.keys.map(k => <sla:notification-msg>{formatProperty(k)}</sla:notification-msg>)}
    {upstreamAppsProperty.keys.map(k => <sla:upstream-apps>{formatProperty(k)}</sla:upstream-apps>)}
  </sla:info>

}
