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
package org.antipathy.scoozie.xml.validator.schema

import org.antipathy.scoozie.xml.validator.xml.Input
import org.apache.commons.io.FilenameUtils
import org.w3c.dom.ls.{LSInput, LSResourceResolver}

/**
  * Utility class to handle schema import and include statements
  */
private[scoozie] class ResourceResolver extends LSResourceResolver {

  /**
    * handle schema import and include statements
    */
  override def resolveResource(typ: String,
                               namespaceURI: String,
                               publicId: String,
                               systemId: String,
                               baseURI: String): LSInput = {
    import org.antipathy.scoozie.Scoozie
    val nonNullSystemId = if (systemId == Scoozie.Null) {
      FilenameUtils.getName(baseURI)
    } else {
      systemId
    }
    val resourceAsStream =
      this.getClass.getClassLoader.getResourceAsStream(nonNullSystemId)
    new Input(publicId, nonNullSystemId, resourceAsStream)
  }
}
// $COVERAGE-ON$
