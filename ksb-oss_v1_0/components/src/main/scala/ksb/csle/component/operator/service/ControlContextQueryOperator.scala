package ksb.csle.component.operator.service

import scala.collection.JavaConversions._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
//import org.joda.convert.FromStringConverter
import org.apache.logging.log4j.scala.Logging

import ksb.csle.common.proto.OndemandControlProto.OnDemandOperatorInfo
import ksb.csle.common.proto.KnowledgeProto.KbControlQueryInfo
import ksb.csle.common.proto.KnowledgeProto.ControlQueryInfo
import ksb.csle.common.proto.KnowledgeProto.Values
import ksb.csle.common.base.operator.BaseGenericMutantOperator
import ksb.csle.common.utils.ProtoUtils.msgToJson
import ksb.csle.common.utils.response.KbeResponse
import ksb.csle.common.utils.kbe.KbeForwarder

import spray.json._
import DefaultJsonProtocol._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that queries recommanded device control with pipelined data that passed
 * from predictoin engine.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.OndemandOperatorProto.KbControlQueryInfo]]
 *          KbControlQueryInfo contains attributes as follows:
 *          - uri: Host uri to query (required)
 *          - controlQuery: Message object that specifies query format
 *                          to be passed to host (required)
 *                          If the field value in control query is '?',
 *                          pipelined data will be passed in that field as value
 *                          Detailed information is as follows:
 *
 *  ==KbControlQueryInfo==
 * {{{
 *  message KbControlQueryInfo {
 *  required string uri = 1 [default = "http://0.0.0.0:9876/recommendDeviceControl"];
 *  required ControlQueryInfo controlQuery = 2;
 *  }
 * }}}
 *
 *          - thingId: Thing id.
 *          - resourceId: Resource id.
 *          - time: Time information.
 *          - values: Nested key-value pairs.
 *
 *  ==ControlQueryInfo==
 * {{{
 * message ControlQueryInfo {
 *  optional string thingId = 1;
 *  optional string resourceId = 2;
 *  optional string time = 3;
 *  repeated Values values = 4;
 * }
 * }}}
 *
 *  ==Example data of ControlQueryInfo==
 *  {{{
 *  {
 *    "thingId": "a1234",
 *    "resourceId": "temperature",
 *    "time": "20170330103347",
 *    "values": [
 *       {
 *          "name": "temperature",
 *          "value": "32.6"
 *         }
 *       ]
 *    }
 *  }}}
 *
 *  ==Example data of ControlQueryInfo==
 *  {{{
 *  {
 *    "thingId": "a1234",
 *    "resourceId": "temperature",
 *    "time": "?",  // current time will be set in runtime.
 *    "values": [
 *       {
 *          "name": "temperature",
 *          "value": "?" // pipelined data will be set in runtime.
 *         }
 *       ]
 *    }
 *  }}}
 */
final case class PredictResult(predicted_label: Double)

object MyJsonProtocol extends DefaultJsonProtocol {
 implicit val PredictResultFormat: spray.json.RootJsonFormat[ksb.csle.component.operator.service.PredictResult] = jsonFormat1(PredictResult)
}

class ControlContextQueryOperator(
    o: OnDemandOperatorInfo) extends BaseGenericMutantOperator[OnDemandOperatorInfo, Option[String], Option[String] => Option[KbeResponse]](o){

import MyJsonProtocol._

  val p: KbControlQueryInfo = o.getKbControlQuery
  val cquery: ControlQueryInfo = p.getControlQuery
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern("yyyymmddHHmmss");
  var queuedPrediction: Option[String] = None

  private def query(data: Option[String]): Option[String] => Option[KbeResponse] = { data =>
    logger.info(s"PredictedResult Query: ${data}")

    queuedPrediction = data match {
      case Some(s) =>
        Some(s.parseJson.convertTo[PredictResult].predicted_label.toString)
      case None =>
        queuedPrediction
    }
    queuedPrediction match {
      case Some(s) =>
        val values = cquery.getValuesList.map( v =>
          Values.newBuilder()
         .setName(v.getName)
         .setValue(queuedPrediction.get)
         .build()).toList
        val content = ControlQueryInfo.newBuilder()
          .setThingId(cquery.getThingId)
          .setResourceId(cquery.getResourceId)
          .setTime(DateTime.now().toString(dtf))
          .addAllValues(values)
          .build
        val msg = msgToJson(content)
        logger.info(s"Context Query: ${msg}")
        Some(KbeForwarder.forwarderPost(p.getUri, msg))
      case None =>
        Some(KbeResponse(-1, "NONE"))
    }
  }

  /**
   * Operates prediction with SparkML Model.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(
      value: Option[String]): Option[String] => Option[KbeResponse] = query(value)
}

object ControlContextQueryOperator extends Logging {
  def apply(o: OnDemandOperatorInfo): ControlContextQueryOperator = new ControlContextQueryOperator(o)
}
