package ksb.csle.component.ingestion.util

import org.apache.commons.codec.binary._

/**
 * Utility class to support oneM2M.
 */
object OneM2MUtils {

  /**
   * Extract content from oneM2M notification message.
   *
   * @param  notification Notification message.
   * @param  contentType  Content-Type of Notification message.
   * @return String       Content in the notification message.
   */
  def extractContent(notification: String, contentType: String): String = {
    contentType match {
      case "application/json" | "application/vnd.onem2m-ntfy+json" =>
        extractContentFromJson(notification)
      case _ =>
        throw new RuntimeException(
            s"Not supported Content-Type: '${contentType}'.")
    }
  }

  private def extractContentFromJson(jsonText: String): String = {
    import spray.json._

    val ntfy = jsonText.parseJson.asJsObject
    val sgn = ntfy.getFields("m2m:sgn")(0)
    val nev = sgn.asJsObject.getFields("nev")(0)
    val rep = nev.asJsObject.getFields("rep")(0)
    val cin = rep.asJsObject.getFields("m2m:cin")(0)
    val cnf = cin.asJsObject.getFields("cnf")(0)
    val con = cin.asJsObject.getFields("con")(0)

    val contentInfo = cnf.asInstanceOf[JsString].value
    val content = con.asInstanceOf[JsString].value
    val delimiterIdx = contentInfo.indexOf(':')
    val contentType = contentInfo.substring(0, delimiterIdx)
    val encodingType = contentInfo.substring(delimiterIdx + 1)

    decode(content, encodingType)
  }

  private def decode(encodedData: String, encodingType: String): String = {
    encodingType match {
      case "1" => // base64 string.
        StringUtils.newStringUtf8(Base64.decodeBase64(encodedData))
      case "2" => // base64 binary.
        throw new RuntimeException(
            "Not supported encodingType: base64 binary.")
      case _ => // plain.
        encodedData
    }
  }
}
