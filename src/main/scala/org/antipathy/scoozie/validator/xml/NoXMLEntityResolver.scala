package org.antipathy.scoozie.validator.xml

import java.io.IOException
import org.apache.xerces.xni.XMLResourceIdentifier
import org.apache.xerces.xni.parser.{XMLEntityResolver, XMLInputSource}

/**
  * Class for rejecting plain XML documents
  */
private[scoozie] class NoXMLEntityResolver extends XMLEntityResolver {

  /**
    * Raise an error when DOCTYPE element is found
    */
  override def resolveEntity(xmlResourceIdentifier: XMLResourceIdentifier): XMLInputSource =
    throw new IOException(
      "DOCTYPE is disallowed when the feature http://apache.org/xml/features/disallow-doctype-decl " +
      "set to true."
    )
}
