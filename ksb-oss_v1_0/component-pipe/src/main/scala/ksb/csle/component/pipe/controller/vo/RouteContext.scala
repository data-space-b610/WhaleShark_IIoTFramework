package ksb.csle.component.pipe.controller.vo

import scala.util.{Try, Success, Failure}

import spray.json._

import ksb.csle.common.base.param.BaseParam
import ksb.csle.common.base.param.BaseRouteContext

trait BaseCase extends BaseParam {
  def data: Option[String]
  def route: Option[String]
  def input: Option[String]
  def output: Option[String]
}

case class RouteCtxCase(
    var data: Option[String],
    var route: Option[String],
    var input: Option[String],
    var output: Option[String]) extends BaseCase

object MyRouteJsonProtocol extends DefaultJsonProtocol {
  implicit val RouteCtxCaseFormat = jsonFormat4(RouteCtxCase)
}

class RouteContext(override val myCtxCase: RouteCtxCase) extends BaseRouteContext[RouteCtxCase](myCtxCase) {

  import ksb.csle.component.pipe.controller.vo.MyRouteJsonProtocol._

  def this(yourCtxStr: String)(
      implicit ev: spray.json.JsonReader[ksb.csle.component.pipe.controller.vo.RouteCtxCase]) = {
    this(yourCtxStr.asJson.convertTo[RouteCtxCase])
  }

  override def copy = new RouteContext(
      RouteCtxCase(myCtxCase.data, myCtxCase.input, myCtxCase.input, myCtxCase.output))

  override def setData(data: Option[String]) = {
    myCtxCase.data = data
  }

  override def getData = myCtxCase.data

  override def setRoute(route: Option[String]) = {
    myCtxCase.route = route
  }

  override def getRoute = myCtxCase.route

  override def setInput(input: Option[String]) = {
    myCtxCase.input = input
  }

  override def getInput = myCtxCase.input

  override def setOutput(output: Option[String]) = {
    myCtxCase.output = output
  }

  override def getOutput = {
    myCtxCase.output
  }

  override def toCase(yourCtxStr: String) = RouteContext.toCase(yourCtxStr)

  override def toJsonString = myCtxCase.toJson.toString()
}

object RouteContext {
  def toCase(jsonStr: String)(
      implicit ev: spray.json.JsonReader[ksb.csle.component.pipe.controller.vo.RouteCtxCase]) = {
    Try {
      jsonStr.asJson.convertTo[RouteCtxCase]
    } match {
      case Success(c) => c
      case Failure(e) => new RouteCtxCase(Some(jsonStr), None, Some(jsonStr), None)
    }
  }
}
