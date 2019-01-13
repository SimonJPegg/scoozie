package org.antipathy.scoozie.validator.xml

import java.io.{BufferedInputStream, IOException, InputStream, Reader}
import org.w3c.dom.ls.LSInput

/**
  * Utility class to handle schema import and include statements
  */
private[scoozie] class Input(var publicId: String,
                             var systemId: String,
                             val input: InputStream)
    extends LSInput {
  private val inputStream = new BufferedInputStream(input)
  private val UTF_8_ENCODING = "UTF-8"

  override def getStringData: String =
    this.synchronized {
      try {
        val input = new Array[Byte](inputStream.available)
        inputStream.read(input)
        val contents = new String(input, UTF_8_ENCODING)
        contents
      } catch {
        case e: IOException =>
          e.printStackTrace()
          System.out.println("Exception " + e)
          null
      }
    }

  override def getPublicId: String = publicId

  override def setPublicId(publicId: String): Unit = this.publicId = publicId

  override def getBaseURI: String = null

  override def setBaseURI(baseURI: String): Unit = {}

  override def getByteStream: InputStream = null

  override def setByteStream(byteStream: InputStream): Unit = {}

  override def getCertifiedText = false

  override def setCertifiedText(certifiedText: Boolean): Unit = {}

  override def getCharacterStream: Reader = null

  override def setCharacterStream(characterStream: Reader): Unit = {}

  override def getEncoding: String = null

  override def setEncoding(encoding: String): Unit = {}

  override def setStringData(stringData: String): Unit = {}

  override def getSystemId: String = systemId

  override def setSystemId(systemId: String): Unit = this.systemId = systemId
}
