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

import javax.xml.XMLConstants
import javax.xml.transform.Source
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory, Validator}
import org.antipathy.scoozie.exception.NoSchemaException
import org.antipathy.scoozie.xml.validator.SchemaType
import org.antipathy.scoozie.xml.validator.SchemaType.SchemaType
import org.antipathy.scoozie.xml.validator.xml.NoXMLEntityResolver

import scala.collection.immutable._

/**
  * Re-implementation of the Oozie SchemaService class
  */
private[scoozie] class SchemaService {

  private val xmlEntityResolver = new NoXMLEntityResolver
  private val wfSchema: Schema = loadSchema(SchemaService.wfSchemaNames)
  private val coordSchema: Schema = loadSchema(SchemaService.coOrdSchemaNames)

  /**
    * Returns validator for schema
    */
  def getValidator(schemaName: SchemaType): Validator =
    getValidator(getSchema(schemaName))

  /**
    * Returns validator for schema
    */
  private def getValidator(schema: Schema): Validator = {
    val validator: Validator = schema.newValidator
    validator.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    validator.setFeature("http://xml.org/sax/features/external-general-entities", false)
    validator.setFeature("http://xml.org/sax/features/external-parameter-entities", false)
    validator.setProperty("http://apache.org/xml/properties/internal/entity-resolver", xmlEntityResolver)
    validator
  }

  /**
    * Load the passed in set of schema file names
    */
  private def loadSchema(schemaNames: Set[String]): Schema = {
    val sources = schemaNames.map { schemaName =>
      val s: Source = new StreamSource(
        Thread.currentThread.getContextClassLoader
          .getResourceAsStream(schemaName)
      )
      s.setSystemId(schemaName)
      s
    }.toArray
    schemaFactory.setResourceResolver(new ResourceResolver)
    schemaFactory.newSchema(sources)
  }

  /**
    * Creates schema factory
    */
  private lazy val schemaFactory: SchemaFactory = {
    val factory: SchemaFactory =
      SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
    factory
  }

  /**
    * Return the schema for XML validation of application definitions.
    */
  private def getSchema(schemaName: SchemaType): Schema = schemaName match {
    case SchemaType.coOrdinator => coordSchema
    case SchemaType.workflow    => wfSchema
    case _ =>
      throw new NoSchemaException("No schema found with name " + schemaName)
  }
}

/**
  * Companion object contains related schema objects
  */
object SchemaService {

  val wfSchemaNames = Set("oozie-common-1.0.xsd",
                          "oozie-workflow-0.5.xsd",
                          "shell-action-0.2.xsd",
                          "email-action-0.2.xsd",
                          "hive-action-0.5.xsd",
                          "hive2-action-0.1.xsd",
                          "sqoop-action-0.3.xsd",
                          "ssh-action-0.2.xsd",
                          "distcp-action-0.2.xsd",
                          "oozie-sla-0.2.xsd",
                          "spark-action-0.1.xsd")

  val coOrdSchemaNames = Set("oozie-coordinator-0.4.xsd", "oozie-sla-0.2.xsd")
}
// $COVERAGE-ON$
