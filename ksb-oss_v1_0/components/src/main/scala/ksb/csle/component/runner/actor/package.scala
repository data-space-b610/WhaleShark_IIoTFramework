package ksb.csle.component.runner

import scala.util.{Try, Failure, Random, Success}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.json._

package object actor {
  case class ControlContext(dateTime: DateTime, params: Map[String, String])

  object ControlContextProtocol extends DefaultJsonProtocol {
    implicit object DateTimeFormat extends RootJsonFormat[DateTime] {
      val formatter = ISODateTimeFormat.basicDateTimeNoMillis

      def write(jodaTime: DateTime): JsValue = {
        JsString(formatter.print(jodaTime))
      }

      def read(json: JsValue): DateTime = json match {
        case JsString(s) =>
          Try(formatter.parseDateTime(s)) match {
            case Success(jodaTime) => jodaTime
            case Failure(e) => serializationError("invalid format")
          }
        case _ =>
          serializationError("invalid type")
      }
    }

    implicit object AnyJsonFormat extends JsonFormat[Any] {
        def write(x: Any) = x match {
          case n: Int => JsNumber(n)
          case l: Long => JsNumber(l)
          case d: Double => JsNumber(d)
          case f: Float => JsNumber(f)
          case s: String => JsString(s)
          case b: Boolean if b == true => JsTrue
          case b: Boolean if b == false => JsFalse
          case _ => deserializationError("Not supported data type")
        }

        def read(value: JsValue) = value match {
          case JsNumber(n) if (n.scale > 0) => n.doubleValue()
          case JsNumber(n) if !(n.scale > 0) => n.longValue()
          case JsString(s) => s
          case JsTrue => true
          case JsFalse => false
          case _ => serializationError("Not supported data type")
        }
      }

    implicit val controlContextFormat = jsonFormat2(ControlContext)
  }
}
