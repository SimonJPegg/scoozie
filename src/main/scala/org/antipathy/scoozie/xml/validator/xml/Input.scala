package org.antipathy.scoozie.xml.validator.xml

import java.io.{BufferedInputStream, IOException, InputStream, Reader}

import org.antipathy.scoozie.Scoozie
import org.w3c.dom.ls.LSInput
import scala.util._

/**
  * Utility class to handle schema import and include statements
  */
private[scoozie] class Input(var publicId: String, var systemId: String, val input: InputStream) extends LSInput {
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

  override def setPublicId(publicId: String): Unit = this.publicId = publicId

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

  override def setSystemId(systemId: String): Unit = this.systemId = systemId
}
