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
package org.antipathy.scoozie.action

import org.antipathy.scoozie.configuration.Credentials
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class EmailActionSpec extends FlatSpec with Matchers {

  behavior of "EmailAction"

  it should "generate valid XML" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = EmailAction(name = "email",
                             to = Seq("a@a.com", "b@b.com"),
                             cc = Seq.empty,
                             subject = "message subject",
                             body = "message body",
                             contentTypeOption = None).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<email xmlns="uri:oozie:email-action:0.2">
          <to>{"${email_to}"}</to>
          <subject>{"${email_subject}"}</subject>
          <body>{"${email_body}"}</body>
        </email>))

    result.properties should be(
      Map("${email_to}" -> "a@a.com,b@b.com",
          "${email_subject}" -> "message subject",
          "${email_body}" -> "message body")
    )
  }

  it should "generate valid XML when a CC list is defined" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = EmailAction(name = "email",
                             to = Seq("a@a.com", "b@b.com"),
                             cc = Seq("c@c.com", "d@d.com"),
                             "message subject",
                             "message body",
                             contentTypeOption = None).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<email xmlns="uri:oozie:email-action:0.2">
          <to>{"${email_to}"}</to>
          <cc>{"${email_cc}"}</cc>
          <subject>{"${email_subject}"}</subject>
          <body>{"${email_body}"}</body>
        </email>))

    result.properties should be(
      Map("${email_to}" -> "a@a.com,b@b.com",
          "${email_subject}" -> "message subject",
          "${email_body}" -> "message body",
          "${email_cc}" -> "c@c.com,d@d.com")
    )
  }
}
