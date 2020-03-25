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

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class OozieSLASpec extends FlatSpec with Matchers {

  behavior of "OozieSla"

  it should "generate valid XML" in {

    val actionName = "someAction"
    val result = OozieSLA(nominalTime = "nominal_time",
                          shouldStart = Some("10 * MINUTES"),
                          shouldEnd = Some("30 * MINUTES"),
                          maxDuration = Some("30 * MINUTES"),
                          alertEvents = Seq(StartMiss, EndMiss, DurationMiss),
                          alertContacts = Seq("a@a.com", "b@b.com"),
                          notificationMsg = Some("Some message"),
                          upstreamApps = Seq("app1", "app2")).withActionName(actionName).mappedType

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<sla:info>
        <sla:nominal-time>{"${nominal_time}"}</sla:nominal-time>
        <sla:should-start>{"${someAction_sla_shouldStart}"}</sla:should-start>
        <sla:should-end>{"${someAction_sla_shouldEnd}"}</sla:should-end>
        <sla:max-duration>{"${someAction_sla_maxDuration}"}</sla:max-duration>
        <sla:alert-events>{"${someAction_sla_alertEvents}"}</sla:alert-events>
        <sla:alert-contact>{"${someAction_sla_alertContacts}"}</sla:alert-contact>
        <sla:notification-msg>{"${someAction_sla_notificationMsg}"}</sla:notification-msg>
        <sla:upstream-apps>{"${someAction_sla_upstreamApps}"}</sla:upstream-apps>
    </sla:info>))

    result.properties should be(
      Map("${someAction_sla_maxDuration}" -> "30 * MINUTES",
          "${someAction_sla_shouldEnd}" -> "30 * MINUTES",
          "${someAction_sla_notificationMsg}" -> "Some message",
          "${someAction_sla_upstreamApps}" -> "app1,app2",
          "${someAction_sla_alertEvents}" -> "start_miss,end_miss,duration_miss",
          "${someAction_sla_alertContacts}" -> "a@a.com,b@b.com",
          "${someAction_sla_shouldStart}" -> "10 * MINUTES")
    )
  }

}
