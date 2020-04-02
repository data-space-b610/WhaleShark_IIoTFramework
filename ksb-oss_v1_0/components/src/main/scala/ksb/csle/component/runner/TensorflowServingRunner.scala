package ksb.csle.component.runner

import java.io.{BufferedReader, StringReader}

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._

import com.google.protobuf.Message

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.StandardRoute

import org.tensorflow.framework._

import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.RunnerProto.OnDemandRunnerInfo
import ksb.csle.common.proto.RunnerProto.TensorflowServingRunnerInfo._
import ksb.csle.common.utils.tensorflow._

import ksb.csle.component.operator.service.TensorflowServingOperator

import _root_.tensorflow.serving.GetModelMetadata._
import _root_.tensorflow.serving.Predict._

import TensorflowServingRunner._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Runner that serves a tensorflow model and provides RESTful APIs.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.RunnerProto.TensorflowServingRunnerInfo]]
 *          TensorflowServingRunnerInfo containing attributes are as follows:
 *          - port: Port number for RESTful APIs (required)
 *          - modelName: Model name (required)
 *          - modelBasePath: Model location (required)
 *                           if in the local file system, set '/model/base'.
 *                           if in the HDFS, set 'hdfs://ip:port/model/base'.
 *                           if gRPC model server, set 'grpc://ip:port'.
 *          - options: options of tensorflow model serving (repeated)
 *
 * ==TensorflowServingRunnerInfo==
 * {{{
 * message TensorflowServingRunnerInfo {
 *   required int32 port = 1 [default = 8001];
 *   required string modelName = 2 [default = "default"];
 *   required string modelBasePath = 3 [default = "/your/model/base"];
 *   repeated ServingOption options = 4;
 * }
 * }}}
 */
final class TensorflowServingRunner(
    o: OnDemandRunnerInfo
    ) extends BaseRunner[
      ModelSession, OnDemandRunnerInfo, ModelServingState](o) {

  private[this] var servingOp: TensorflowServingOperator = null

  private[this] val info =
    if (o.getTfServingRunner == null) {
      throw new IllegalArgumentException(
          "TensorflowServingRunnerInfo is not set.")
    } else {
      o.getTfServingRunner
    }

  private[this] val options = info.getOptionsList.map { o =>
    o.getKey -> o.getValue
  }.toMap

  private[this] val format = new MessageFormat()

  private[this] val modelServer = ModelServerFactory.create(
      info.getModelName(), info.getModelBasePath(), options)

  private[this] implicit val system = ActorSystem(
      s"tensorflow-serving-rest-${info.getPort()}")
  private[this] implicit val materializer = ActorMaterializer()
  private[this] implicit val executionContext = system.dispatcher

  private[this] val apiRoute = withoutSizeLimit {
    inputRoute ~ signatureRoute ~ modelRoute
  }
  private[this] val bindingFuture = Http().bindAndHandle(
      apiRoute, "0.0.0.0", info.getPort())

  /**
   * Get the session to access the model server.
   *
   * @return ModelSession Session to access the model server
   */
  override def getSession: ModelSession = modelServer

  /**
   * Initialize the runner for tensorflow model serving.
   *
   * @param  op                TensorflowServingOperator instance.
   * @return ModelServingState State of model serving
   */
  override def init(op: Any): ModelServingState = {
    if (modelServer.available() == false) {
      throw new RuntimeException("ModelServer is not available")
    }

    if (op == null) {
      throw new RuntimeException("Serving Operator is not set.")
    } else {
      try {
        servingOp = op.asInstanceOf[TensorflowServingOperator]
      } catch {
        case e: ClassCastException =>
          throw new RuntimeException(
              s"Serving Operator is not valid type: ${e.getMessage()}")
      }
    }

    ModelServingState("initialized")
  }

  /**
   * Start the runner for tensorflow model serving.
   *
   * @return ModelServingState State of model serving
   */
  override def run(a: Any): ModelServingState = {
    logger.info(s"Tensorflow model is loaded: $modelServer")
    logger.info(s"Tensorflow serving is ready: port=${info.getPort()}")

    ModelServingState("started")
  }

  /**
   * Stop the runner.
   */
  override def stop {
    if (bindingFuture != null) {
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => system.terminate())
    }

    modelServer.shutdown()

    ModelServingState("stopped")
  }

  private def modelRoute = path("model") {
    get {
      getModelMetadata()
    } ~
    (post & parameters('base64 ? false)) { base64 =>
      extractRequest { request =>
        request.entity.contentType.mediaType.value match {
          case "application/json" =>
            entity(as[String]) { json =>
              predict(json, base64)
            }
          case "application/octet-stream" =>
            entity(as[Array[Byte]]) { binary =>
              predict(binary, base64)
            }
          case _ =>
            complete { BadRequest ->
              format.toJson("not supported Content-Type: "
                  + request.entity.contentType.mediaType.value)
            }
        }
      }
    }
  }

  private def getModelMetadata(): StandardRoute = {
    Try {
      servingOp.operate(ModelMetadataRequestContext(modelServer))
    } match {
      case Success(ctx) =>
        complete { OK->
          HttpEntity(ContentTypes.`application/json`, format.toJson(ctx))
        }
      case Failure(e) =>
        e match {
          case bad: BadArgument =>
            complete { BadRequest ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(bad))
            }
          case other: Exception =>
            complete { InternalServerError ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(other))
            }
        }
    }
  }

  private def predict(content: Any, base64: Boolean): StandardRoute = {
    import com.google.protobuf.InvalidProtocolBufferException

    Try {
      servingOp.operate {
        content match {
          case json: String => PredictRequestContext(
              modelServer, format.toPredictRequest(json), base64)
          case binary: Array[Byte] => PredictRequestContext(
              modelServer, format.toPredictRequest(binary), base64)
        }
      }
    } match {
      case Success(ctx) =>
        content match {
          case _: Array[Byte] =>
            complete { OK ->
              HttpEntity(ContentTypes.`application/octet-stream`,
                  format.toBinary(ctx))
            }
          case _ =>
            complete { OK ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(ctx))
            }
        }
      case Failure(e) =>
        e match {
          case bad @ (_: InvalidProtocolBufferException | _: BadArgument) =>
            complete { BadRequest ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(bad))
            }
          case other: Exception =>
            complete { InternalServerError ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(other))
            }
        }
    }
  }

  private def signatureRoute = pathPrefix("model" / Segment) {
    name =>
      get {
        getSignatureDef(name)
      } ~
      (post & parameters('base64 ? false)) { base64 =>
        extractRequest { request =>
          val mediaType = request.entity.contentType.mediaType
          val mainType = mediaType.mainType
          val subType = mediaType.subType
          (mainType, subType) match {
            case ("multipart", "form-data") =>
              entity(as[String]) { str =>
                predict(name, mediaType.value, str, base64)
              }
            case _ =>
              complete { BadRequest ->
                format.toJson("not supported Content-Type: "
                    + request.entity.contentType.mediaType.value)
              }
          }
        }
      }
  }

  private def getSignatureDef(name: String): StandardRoute = {
    Try {
      servingOp.operate(SignatureDefRequestContext(modelServer, name))
    } match {
      case Success(ctx) =>
        complete { OK ->
          HttpEntity(ContentTypes.`application/json`, format.toJson(ctx))
        }
      case Failure(e) =>
        e match {
          case bad: BadArgument =>
            complete { BadRequest ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(bad))
            }
          case other: Exception =>
            complete { InternalServerError ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(other))
            }
        }
    }
  }

  private def predict(name: String, mediaType: String, content: String,
      base64: Boolean): StandardRoute = {
    Try {
      val tensorableContents = FormdataParser
        .parse(mediaType, content)
        .map { case (name, (mty, con)) =>
          (name, new TensorableContent(mty, con))
        }
      val message = PredictParams(name, tensorableContents)
      servingOp.operate(PredictRequestContext(modelServer, message, base64))
    } match {
      case Success(ctx) =>
        complete { OK ->
          HttpEntity(ContentTypes.`application/json`, format.toJson(ctx))
        }
      case Failure(e) =>
        e match {
          case bad @ (_: IllegalArgumentException | _: BadArgument) =>
            complete { NotFound ->
              HttpEntity(ContentTypes.`application/json`, format.toJson(bad))
            }
          case _ =>
            complete { InternalServerError ->
              HttpEntity(ContentTypes.`application/json`, format.toJson(e))
            }
        }
    }
  }

  private def inputRoute = pathPrefix("model" / Segment / Segment) {
    (signatureName, inputName) =>
      get {
        getInputTensorInfo(signatureName, inputName)
      } ~
      (post & parameters('base64 ? false)) { base64 =>
        extractRequest { request =>
          request.entity.contentType.mediaType.mainType match {
            case "image" | "audio" | "video" =>
              entity(as[Array[Byte]]) { binary =>
                predict(signatureName, inputName,
                    request.entity.contentType.mediaType.value,
                    binary, base64)
              }
            case "text" =>
              entity(as[String]) { str =>
                predict(signatureName, inputName,
                    request.entity.contentType.mediaType.value, str, base64)
              }
            case _ =>
              request.entity.contentType.mediaType.value match {
                case "application/octet-stream" =>
                  entity(as[Array[Byte]]) { binary =>
                    predict(signatureName, inputName,
                        request.entity.contentType.mediaType.value,
                        binary, base64)
                  }
                case _ =>
                  complete { BadRequest ->
                    format.toJson("not supported Content-Type: "
                        + request.entity.contentType.mediaType.value)
                  }
              }
          }
        }
      }
  }

  private def getInputTensorInfo(signatureName: String,
      inputName: String): StandardRoute = {
    Try {
      servingOp.operate(InputTensorInfoRequestContext(
          modelServer, signatureName, inputName))
    } match {
      case Success(ctx) =>
        complete { OK ->
          HttpEntity(ContentTypes.`application/json`, format.toJson(ctx))
        }
      case Failure(e) =>
        e match {
          case bad: BadArgument =>
            complete { BadRequest ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(bad))
            }
          case other: Exception =>
            complete { InternalServerError ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(other))
            }
        }
    }
  }

  private def predict[T](signatureName: String, inputName: String,
      contentType: String, content: T, base64: Boolean): StandardRoute = {
    Try {
      val message = PredictParams(
          signatureName,
          Map(inputName -> new TensorableContent(contentType, content)))
      servingOp.operate(PredictRequestContext(modelServer, message, base64))
    } match {
      case Success(ctx) =>
        complete { OK ->
          HttpEntity(ContentTypes.`application/json`, format.toJson(ctx))
        }
      case Failure(e) =>
        e match {
          case bad: BadArgument =>
            complete { BadRequest ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(bad))
            }
          case other: Exception =>
            complete { InternalServerError ->
              HttpEntity(ContentTypes.`application/json`,
                  format.toJson(other))
            }
        }
    }
  }
}

object TensorflowServingRunner {
  object FormdataParser {
    def parse(mediaType: String,
        content: String): Map[String, (String, String)] = {
      val map = collection.mutable.HashMap.empty[String, (String, String)]

      val (boundaryMarker, partMarker, eocMarker) = buildMarker(mediaType)

      var eoc = false
      val reader = new BufferedReader(new StringReader(content))
      do {
        val line = reader.readLine()
        if (line == null) {
          eoc = true
        } else {
          if (line.equals(partMarker)) {
            val partName = parsePartialContentName(reader)
            val partType = parsePartialMediaType(reader)
            val partContent = parsePartialContent(partType, reader)
            map.put(partName, (partType, partContent))
          } else if (line.equals(eocMarker)) {
            eoc = true
          }
        }
      } while (eoc == false)
      reader.close()

      map.toMap
    }

    private def buildMarker(
        contentType: String): (String, String, String) = {
      val begin = contentType.indexOf("boundary=") + 9
      if (begin < 0) {
        throw new IllegalArgumentException(
            s"illegal Content-Type: no boundary")
      }

      val boundaryMarker = contentType.substring(begin)
      val partMarker = "--" + boundaryMarker
      val eocMarker = partMarker + "--"

      (boundaryMarker, partMarker, eocMarker)
    }

    private def parsePartialContentName(reader: BufferedReader): String = {
      val contentDispos = reader.readLine()
      if (contentDispos.isEmpty()) {
        throw new IllegalArgumentException(
            "partial 'Content-Disposition' is not set.")
      }

      val begin = contentDispos.indexOf("name=\"") + 6
      if (begin < 0) {
        throw new IllegalArgumentException(
            s"illegal Content-Disposition: $contentDispos")
      }

      val end = contentDispos.indexOf('"', begin)
      if (end < 0) {
        throw new IllegalArgumentException(
            s"illegal Content-Disposition: $contentDispos")
      }

      contentDispos.substring(begin, end).trim()
    }

    private def parsePartialMediaType(reader: BufferedReader): String = {
      val contentType = reader.readLine()
      if (contentType.isEmpty()) {
        throw new IllegalArgumentException(
            "partial Content-Type is not set.")
      }

      val begin = contentType.indexOf("Content-Type:") + 13
      if (begin < 0) {
        throw new IllegalArgumentException(
            s"illegal Content-Type: $contentType")
      }

      val end = contentType.indexOf(";", begin)
      if (end < 0) {
        contentType.substring(begin).trim()
      } else {
        contentType.substring(begin, end).trim()
      }
    }

    private def parsePartialContent(contentType: String,
        reader: BufferedReader): String = {
      val types = contentType.split("/")
      val mainType = types(0)
      val subType = types(1)

      mainType match {
        case "text" => parsePartialStringContent(reader)
        case _ => throw new IllegalArgumentException(
            s"not supported partial Content-Type: '$contentType'")
      }
    }

    private def parsePartialStringContent(reader: BufferedReader): String = {
      var line = reader.readLine()
      if (line == null) {
        throw new IllegalArgumentException("partial content is not set.")
      }

      val buf =
        if (line.isEmpty()) new StringBuilder()
        else new StringBuilder(line)

      var eoc = false
      do {
        line = reader.readLine()
        if (line == null || line.isEmpty()) {
          eoc = true
        } else {
          buf.append(line)
          buf.append('\n')
        }
      } while (eoc == false)

      buf.toString()
    }
  }
}
