/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
package org.antipathy.scoozie.sla

import com.typesafe.config.Config
import org.antipathy.scoozie.action.Nameable
import org.antipathy.scoozie.builder.{ConfigurationBuilder, HoconConstants, MonadBuilder}
import org.antipathy.scoozie.configuration.ActionProperties
import org.antipathy.scoozie.exception.InvalidConfigurationException
import org.antipathy.scoozie.properties.OozieProperties
import org.antipathy.scoozie.xml.XmlSerializable

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
                    shouldStart: Option[String] = None,
                    shouldEnd: Option[String] = None,
                    maxDuration: Option[String] = None,
                    alertEvents: Seq[SLAAlert] = Seq.empty[SLAAlert],
                    alertContacts: Seq[String] = Seq.empty[String],
                    notificationMsg: Option[String] = None,
                    upstreamApps: Seq[String] = Seq.empty[String],
                    override val name: String = "_sla")
    extends XmlSerializable
    with OozieProperties
    with Nameable {

  private val shouldStartProperty = buildStringOptionProperty(name, "shouldStart", shouldStart)
  private val shouldEndProperty = buildStringOptionProperty(name, "shouldEnd", shouldEnd)
  private val maxDurationProperty = buildStringOptionProperty(name, "maxDuration", maxDuration)
  private val alertEventsProperty =
    buildSequenceToSingleValueProperty(name, s"alertEvents", alertEvents.map(_.name))
  private val alertContactsProperty =
    buildSequenceToSingleValueProperty(name, s"alertContacts", alertContacts)
  private val notificationMsgProperty = buildStringOptionProperty(name, s"notificationMsg", notificationMsg)
  private val upstreamAppsProperty =
    buildSequenceToSingleValueProperty(name, s"upstreamApps", upstreamApps)

  /**
    * Add the owning node name to this SLA object
    */
  def withActionName(actionName: String): ActionProperties[OozieSLA] = {
    val mappedSLA = this.copy(name = s"$actionName${this.name}")
    ActionProperties(mappedSLA, mappedSLA.properties)
  }

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
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
    <sla:nominal-time>{formatProperty(nominalTime)}</sla:nominal-time>
    {shouldStartProperty.keys.map(k => <sla:should-start>{formatProperty(k)}</sla:should-start>)}
    {shouldStartProperty.keys.map(k => <sla:should-end>{formatProperty(k)}</sla:should-end>)}
    {maxDurationProperty.keys.map(k => <sla:max-duration>{formatProperty(k)}</sla:max-duration>)}
    {alertEventsProperty.keys.map(k => <sla:alert-events>{formatProperty(k)}</sla:alert-events>)}
    {alertContactsProperty.keys.map(k => <sla:alert-contact>{formatProperty(k)}</sla:alert-contact>)}
    {notificationMsgProperty.keys.map(k => <sla:notification-msg>{formatProperty(k)}</sla:notification-msg>)}
    {upstreamAppsProperty.keys.map(k => <sla:upstream-apps>{formatProperty(k)}</sla:upstream-apps>)}
  </sla:info>
}

object OozieSLA {

  def apply(config: Config, ownerName: String): OozieSLA =
    MonadBuilder.tryOperation { () =>
      OozieSLA(nominalTime = config.getString(HoconConstants.nominalTime),
               shouldStart = ConfigurationBuilder.optionalString(config, HoconConstants.shouldStart),
               shouldEnd = ConfigurationBuilder.optionalString(config, HoconConstants.shouldEnd),
               maxDuration = ConfigurationBuilder.optionalString(config, HoconConstants.maxDuration),
               alertEvents = ConfigurationBuilder
                 .optionalStringCollection(config, HoconConstants.alertEvents)
                 .map(SLAAlert.fromName),
               alertContacts = ConfigurationBuilder.optionalStringCollection(config, HoconConstants.alertContacts),
               notificationMsg = ConfigurationBuilder.optionalString(config, HoconConstants.notificationMsg),
               upstreamApps = ConfigurationBuilder.optionalStringCollection(config, HoconConstants.upstreamApps))
    } { e: Throwable =>
      new InvalidConfigurationException(s"${e.getMessage} in SLA contstruction for $ownerName", e)
    }
}
