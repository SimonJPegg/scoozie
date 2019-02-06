package org.antipathy.scoozie.xml.validator

import java.io.StringReader
import org.antipathy.scoozie.xml.validator.SchemaType.SchemaType
import javax.xml.transform.stream.StreamSource
import org.antipathy.scoozie.xml.validator.schema.SchemaService

/**
  * Class for validating Oozie artifacts files against an XSD
  */
private[scoozie] object OozieValidator {

  /**
    * Validate the passed in workflow against the passed in schema
    *
    * @param xmlContents The Xml to validate
    * @param schemaType The type of schema to validate
    */
  def validate(xmlContents: String, schemaType: SchemaType): Unit = {

    val service = new SchemaService
    val validator = service.getValidator(schemaType)
    validator.validate(new StreamSource(new StringReader(xmlContents)))
  }
}
