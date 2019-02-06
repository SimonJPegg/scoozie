package org.antipathy.scoozie.action

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration.Credentials
import scala.collection.immutable._

class EmailActionSpec extends FlatSpec with Matchers {

  behavior of "EmailAction"

  it should "generate valid XML" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = EmailAction(name = "email",
                             to = Seq("a@a.com", "b@b.com"),
                             subject = "message subject",
                             body = "message body").action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<email xmlns="uri:oozie:email-action:0.1">
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
                             "message body").action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<email xmlns="uri:oozie:email-action:0.1">
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
