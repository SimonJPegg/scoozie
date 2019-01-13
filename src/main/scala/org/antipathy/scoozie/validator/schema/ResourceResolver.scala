package org.antipathy.scoozie.validator.schema

import org.antipathy.scoozie.validator.xml.Input
import org.apache.commons.io.FilenameUtils
import org.w3c.dom.ls.{LSInput, LSResourceResolver}

/**
  * Utility class to handle schema import and include statements
  */
private[scoozie] class ResourceResolver extends LSResourceResolver {

  /**
    * handle schema import and include statements
    */
  override def resolveResource(`type`: String,
                               namespaceURI: String,
                               publicId: String,
                               systemId: String,
                               baseURI: String): LSInput = {
    val nonNullSystemId = if (systemId == null) {
      FilenameUtils.getName(baseURI)
    } else {
      systemId
    }
    val resourceAsStream =
      this.getClass.getClassLoader.getResourceAsStream(nonNullSystemId)
    new Input(publicId, nonNullSystemId, resourceAsStream)
  }
}
