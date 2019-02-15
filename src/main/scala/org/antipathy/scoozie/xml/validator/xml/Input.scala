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

import java.io.{BufferedInputStream, InputStream, Reader}

import org.antipathy.scoozie.Scoozie
import org.w3c.dom.ls.LSInput

import scala.util._

/**
  * Utility class to handle schema import and include statements
  */
private[scoozie] class Input(val publicId: String, val systemId: String, val input: InputStream) extends LSInput {
  private val inputStream = new BufferedInputStream(input)
  private val UTF_8_ENCODING = "UTF-8"

  override def getStringData: String =
    this.synchronized {
      Try {
        val input = new Array[Byte](inputStream.available)
        inputStream.read(input)
        val contents = new String(input, UTF_8_ENCODING)
        contents
      } match {
        case Success(value) => value
        case Failure(exception) =>
          exception.printStackTrace()
          System.out.println("Exception " + exception)
          Scoozie.Null
      }
    }

  override def getPublicId: String = publicId

  override def setPublicId(publicId: String): Unit = {}

  override def getBaseURI: String = Scoozie.Null

  override def setBaseURI(baseURI: String): Unit = {}

  override def getByteStream: InputStream = Scoozie.Null

  override def setByteStream(byteStream: InputStream): Unit = {}

  override def getCertifiedText = false

  override def setCertifiedText(certifiedText: Boolean): Unit = {}

  override def getCharacterStream: Reader = Scoozie.Null

  override def setCharacterStream(characterStream: Reader): Unit = {}

  override def getEncoding: String = Scoozie.Null

  override def setEncoding(encoding: String): Unit = {}

  override def setStringData(stringData: String): Unit = {}

  override def getSystemId: String = systemId

  override def setSystemId(systemId: String): Unit = {}
}
// $COVERAGE-ON$
