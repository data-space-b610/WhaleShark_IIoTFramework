package ksb.csle.component.pipe.controller.vo

import scala.util.{Try, Success, Failure}

import spray.json._

import ksb.csle.common.base.param.BaseParam
import ksb.csle.common.proto.OndemandControlProto.OutputAggregatePipeOperatorInfo.Method
import ksb.csle.common.proto.OndemandControlProto.OutputAggregatePipeOperatorInfo.Method._
import ksb.csle.common.base.param.BaseCumulativeContext

trait BaseCumulativeCase extends BaseParam {
  def data: Option[Array[String]]
  def route: Option[String]
  def input: Option[Array[String]]
  def output: Option[Array[String]]
}

sealed case class EnsembleCase(
    var data: Option[Array[String]],
    var route: Option[String],
    var input: Option[Array[String]],
    var output: Option[Array[String]]) extends BaseCumulativeCase

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val EnsembleCaseFormat = jsonFormat4(EnsembleCase)
}

class CumulativeContext(
    override val myCtxCase: EnsembleCase
    ) extends BaseCumulativeContext[EnsembleCase, String, Double](myCtxCase) {

  import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._

  def this(yourCtxStr: String)(
      implicit ev: spray.json.JsonReader[ksb.csle.component.pipe.controller.vo.EnsembleCase]){
    this(yourCtxStr.asJson.convertTo[EnsembleCase])
  }

  override def copy = new CumulativeContext(
      EnsembleCase(myCtxCase.data, myCtxCase.route, myCtxCase.input, myCtxCase.output))

  override def getData = Try {
    Some(myCtxCase.data.get.last)
  } match {
    case Success(data) => data
    case Failure(e) => None
  }

  override def setData(data: Option[String]) = data match {
    case Some(e) =>
       myCtxCase.data =  Some(Array(e))
    case _ =>
  }

  override def setRoute(route: Option[String]) = {
    myCtxCase.route = route
  }

  override def getRoute = myCtxCase.route

  override def setInput(input: Option[String]) = input match {
    case Some(e) =>
       myCtxCase.input =  Some(Array(e))
    case _ =>
  }

  override def getInput = Some(myCtxCase.input.get.head)

  override def setOutput(output: Option[String]) = output match {
    case Some(e) =>
       myCtxCase.output =  Some(Array(e))
    case _ =>
  }
  override def getOutput: Option[String] = Some(myCtxCase.output.get.last)

  override def getOutputList: Option[String] = Some(myCtxCase.output.get.mkString(" "))

  override def appendOutput(output: Option[String]): Unit =
    output match {
        case Some(item) =>
          myCtxCase.output =
             myCtxCase.output match {
               case Some(arr) => Some(arr :+ item)
               case None => Some(Array(item))
             }
        case None =>
  }

  override def aggregateOutput(
      method: Method, parser: scala.Function1[String,Array[Double]]
      ): BaseCumulativeContext[EnsembleCase, String, Double] =
        myCtxCase.output match {
          case Some(e) =>
            new CumulativeContext(
                EnsembleCase(myCtxCase.data, myCtxCase.route, myCtxCase.input, Some(aggregateData(e, method, parser))))
          case None =>
            new CumulativeContext(myCtxCase)
        }

  override def aggregateSum(items: Array[Double]): Double = items.foldLeft(0D)(_+_)

  override def aggregateAvg(items: Array[Double]): Double = aggregateSum(items)/items.size

  override def aggregateMax(items: Array[Double]): Double = items.max

  override def aggregateMin(items: Array[Double]): Double = items.min

  override def aggregateAnd(items: Array[Double]): Double = items.head

  override def aggregateOr(items: Array[Double]): Double = items.head

  override def toJsonString = myCtxCase.toJson.toString()

  override def toCase(jsonStr: String) = jsonStr.asJson.convertTo[EnsembleCase]
}
