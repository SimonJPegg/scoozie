package org.antipathy.scoozie.action

import scala.xml.Elem
import org.antipathy.scoozie.configuration.Credentials
import scala.collection.immutable._

/**
  * Email action definition
  * @param name the name of the action
  * @param to the to recipient list
  * @param cc an optional cc recipient list
  * @param subject the message subject
  * @param body the message body
  */
final class EmailAction(override val name: String,
                        to: Seq[String],
                        cc: Seq[String] = Seq.empty[String],
                        subject: String,
                        body: String)
    extends Action {

  private val toProperty = formatProperty(s"${name}_to")
  private val subjectProperty = formatProperty(s"${name}_subject")
  private val bodyProperty = formatProperty(s"${name}_body")
  private val ccProperty = formatProperty(s"${name}_cc")

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:email-action:0.1")

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(toProperty -> to.mkString(","), subjectProperty -> subject, bodyProperty -> body) ++
    (if (cc.nonEmpty) {
       Map(ccProperty -> cc.mkString(","))
     } else { Map() })

  /**
    * The XML for this node
    */
  override def toXML: Elem = <email xmlns={xmlns.orNull}>
        <to>{toProperty}</to>
        { if (cc.nonEmpty) {
            <cc>{ccProperty}</cc>
          }
        }
        <subject>{subjectProperty}</subject>
        <body>{bodyProperty}</body>
      </email>
}

object EmailAction {

  def apply(name: String, to: Seq[String], cc: Seq[String] = Seq.empty[String], subject: String, body: String)(
      implicit credentialsOption: Option[Credentials]
  ): Node =
    Node(new EmailAction(name, to, cc, subject, body))
}
