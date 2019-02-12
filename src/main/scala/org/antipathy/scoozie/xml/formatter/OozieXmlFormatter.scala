package org.antipathy.scoozie.xml.formatter

import org.antipathy.scoozie.xml.XmlSerializable

import scala.xml.PrettyPrinter

/**
  * class for formatting XML documents
  * @param width maximum width of any row
  * @param step indentation for each level of the XML
  */
class OozieXmlFormatter(width: Int, step: Int) extends Formatter[XmlSerializable] {

  val inner: PrettyPrinter = new scala.xml.PrettyPrinter(width, step)

  /**
    * Method for formatting XML nodes
    *
    * @param oozieNode the node to format
    * @return XML document in string format
    */
  def format(oozieNode: XmlSerializable): String =
    inner.format(oozieNode.toXML)
}
