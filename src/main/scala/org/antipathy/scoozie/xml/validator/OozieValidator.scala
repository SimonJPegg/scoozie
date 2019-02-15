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
package org.antipathy.scoozie.xml.validator

import java.io.StringReader

import javax.xml.transform.stream.StreamSource
import org.antipathy.scoozie.xml.validator.SchemaType.SchemaType
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
