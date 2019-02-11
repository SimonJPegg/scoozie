package org.antipathy.scoozie.action

import scala.xml.Elem
import scala.collection.immutable._
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception.ConfigurationMissingException

/**
  * Email action definition
  * @param name the name of the action
  * @param to the to recipient list
  * @param cc an optional cc recipient list
  * @param subject the message subject
  * @param body the message body
  * @param contentTypeOption optional string defining the content type of the message
  */
final class EmailAction(override val name: String,
                        to: Seq[String],
                        cc: Seq[String] = Seq.empty[String],
                        subject: String,
                        body: String,
                        contentTypeOption: Option[String])
    extends Action {

  private val toProperty = formatProperty(s"${name}_to")
  private val subjectProperty = formatProperty(s"${name}_subject")
  private val bodyProperty = formatProperty(s"${name}_body")
  private val ccProperty = formatProperty(s"${name}_cc")
  private val contentTypeProperty = formatProperty(s"${name}_contentType")

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:email-action:0.2")

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(toProperty -> to.mkString(","), subjectProperty -> subject, bodyProperty -> body) ++
    (if (cc.nonEmpty) {
       Map(ccProperty -> cc.mkString(","))
     } else { Map() }) ++
    (if (contentTypeOption.isDefined) {
       Map(contentTypeProperty -> contentTypeOption.get)
     } else { Map() })

  /**
    * Does this action require yarn credentials in Kerberos environments
    */
  override def requiresCredentials: Boolean = false

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
        {if (contentTypeOption.isDefined) {
            <content_type>{contentTypeProperty}</content_type>
          }
        }
      </email>
}

/**
  * Comaption object
  */
object EmailAction {

  /**
    * Create a new instance of this action
    */
  def apply(name: String,
            to: Seq[String],
            cc: Seq[String],
            subject: String,
            body: String,
            contentTypeOption: Option[String]): Node =
    Node(new EmailAction(name, to, cc, subject, body, contentTypeOption))(None)

  /**
    * Create a new instance of this action from a configuration
    */
  def apply(config: Config): Node =
    try {
      EmailAction(to = Seq(config.getStringList("to").asScala: _*),
                  cc = Seq(config.getStringList("cc").asScala: _*),
                  name = config.getString("name"),
                  subject = config.getString("subject"),
                  body = config.getString("body"),
                  contentTypeOption = if (config.hasPath("content-type")) {
                    Some(config.getString("content-type"))
                  } else None)
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${config.getString("name")}")
    }
}
