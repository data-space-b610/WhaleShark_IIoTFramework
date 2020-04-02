package ksb.csle.component.ingestion

import akka.http.scaladsl.model._

package object common {
  /**
   * HTTP ingestion request message.
   */
  case class HttpIngestionRequest(
    content: String,
    contentType: Option[String] = None,
    headers: Option[Map[String, String]] = None)

  /**
   * HTTP ingestion response message.
   */
  case class HttpIngestionResponse(
      data: String,
      statusCode: StatusCode = StatusCodes.OK,
      message: String = "OK")

  /**
   * Default function to handle HTTP ingestion request.
   */
  def defaultHttpIngestionHandler(
      req: HttpIngestionRequest): HttpIngestionResponse = {
    HttpIngestionResponse(req.content)
  }
}
