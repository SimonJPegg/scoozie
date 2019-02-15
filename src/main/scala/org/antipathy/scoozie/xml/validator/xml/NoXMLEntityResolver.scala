// $COVERAGE-OFF$
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
package org.antipathy.scoozie.xml.validator.xml

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
// $COVERAGE-ON$
