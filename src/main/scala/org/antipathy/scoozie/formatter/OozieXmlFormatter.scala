package org.antipathy.scoozie.formatter

import org.antipathy.scoozie.XmlSerializable

/**
  * class for formatting XML documents
  * @param width maximum width of any row
  * @param step indentation for each level of the XML
  */
class OozieXmlFormatter(width: Int, step: Int) extends Formatter[XmlSerializable] {

  /**
    * Method for formatting XML nodes
    *
    * @param oozieNode the node to format
    * @return XML document in string format
    */
  def format(oozieNode: XmlSerializable): String = {
    val formatter = new scala.xml.PrettyPrinter(width, step)
    formatter.format(oozieNode.toXML)
  }
}
