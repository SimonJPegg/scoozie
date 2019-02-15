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

import java.io.File

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.xml.sax.SAXParseException

class OozieValidatorSpec extends FlatSpec with GivenWhenThen with Matchers {

  behavior of "XmlValidator"

  it should "Validate a valid workflow" in {
    OozieValidator.validate(xmlContents = scala.io.Source
                              .fromFile(new File("src/test/resources/xml/valid_workflow.xml"))
                              .getLines()
                              .mkString,
                            schemaType = SchemaType.workflow)
  }

  it should "Validate a valid workflow containing a sub-workflow" in {
    OozieValidator.validate(
      xmlContents = scala.io.Source
        .fromFile(new File("src/test/resources/xml/valid_workflow_with_sub.xml"))
        .getLines()
        .mkString,
      schemaType = SchemaType.workflow
    )
  }

  it should "Validate a valid sub-workflow" in {
    OozieValidator.validate(xmlContents = scala.io.Source
                              .fromFile(new File("src/test/resources/xml/valid_sub_worklfow.xml"))
                              .getLines()
                              .mkString,
                            schemaType = SchemaType.workflow)
  }

  it should "Validate a valid co-ordinator" in {
    OozieValidator.validate(xmlContents = scala.io.Source
                              .fromFile(new File("src/test/resources/xml/valid_coordinator.xml"))
                              .getLines()
                              .mkString,
                            schemaType = SchemaType.coOrdinator)
  }

  it should "Raise an exception on an invalid workflow" in {
    an[SAXParseException] should be thrownBy {
      OozieValidator.validate(xmlContents = scala.io.Source
                                .fromFile(new File("src/test/resources/xml/invalid_workflow.xml"))
                                .getLines()
                                .mkString,
                              schemaType = SchemaType.workflow)
    }
  }

  it should "Raise an exception on an invalid workflow action" in {
    an[SAXParseException] should be thrownBy {
      OozieValidator.validate(
        xmlContents = scala.io.Source
          .fromFile(new File("src/test/resources/xml/invalid_workflow_action.xml"))
          .getLines()
          .mkString,
        schemaType = SchemaType.workflow
      )
    }
  }

  it should "Raise an exception on an invalid co-ordinator" in {
    an[SAXParseException] should be thrownBy {
      OozieValidator.validate(xmlContents = scala.io.Source
                                .fromFile(new File("src/test/resources/xml/invalid_coordinator.xml"))
                                .getLines()
                                .mkString,
                              schemaType = SchemaType.coOrdinator)
    }
  }

  it should "Raise an exception on a broken workflow" in {
    an[SAXParseException] should be thrownBy {
      OozieValidator.validate(xmlContents = scala.io.Source
                                .fromFile(new File("src/test/resources/xml/broken_workflow.xml"))
                                .getLines()
                                .mkString,
                              schemaType = SchemaType.workflow)
    }
  }

  it should "Raise an exception on a broken co-ordinator" in {
    an[SAXParseException] should be thrownBy {
      OozieValidator.validate(xmlContents = scala.io.Source
                                .fromFile(new File("src/test/resources/xml/broken_coordinator.xml"))
                                .getLines()
                                .mkString,
                              schemaType = SchemaType.coOrdinator)
    }
  }
}
