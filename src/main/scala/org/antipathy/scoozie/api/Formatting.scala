package org.antipathy.scoozie.api

import org.antipathy.scoozie.xml.XmlSerializable
import org.antipathy.scoozie.xml.formatter.OozieXmlFormatter

/**
  * Methods for formatting Oozie workflows
  */
private[scoozie] object Formatting {

  private val formatter: OozieXmlFormatter = new OozieXmlFormatter(80, 4)

  /**
    * Method for formatting XML nodes
    *
    * @param oozieNode the node to format
    * @return XML document in string format
    */
  def format(oozieNode: XmlSerializable): String =
    formatter.format(oozieNode)
}
