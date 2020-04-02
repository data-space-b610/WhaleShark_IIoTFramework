package ksb.csle.component.operator.service

import scala.util.{Try, Success, Failure}

import tensorflow.serving.GetModelMetadata._
import tensorflow.serving.Predict._

import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.base.operator.BaseGenericMutantOperator
import ksb.csle.common.utils.tensorflow._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that processes the request of tensorflow model.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.OndemandOperatorProto.TensorflowServingOperatorInfo]]
 *          TensorflowPredictorInfo contains no attribute.
 *
 * ==TensorflowServingOperatorInfo==
 * {{{
 * message TensorflowServingOperatorInfo {
 * }
 * }}}
 */
class TensorflowServingOperator(
    o: OnDemandOperatorInfo
    ) extends BaseGenericMutantOperator[
      OnDemandOperatorInfo, RequestContext, ResponseContext](o) {

  /**
   * Process the request about tensorflow serving.
   * (e.g. retrieving meta data, prediction)
   *
   * @param  requestContext  service request context
   * @return ResponseContext service response context
   */
  override def operate(requestContext: RequestContext): ResponseContext = {
    Try {
      requestContext match {
        case m: ModelMetadataRequestContext =>
          ModelMetadataResponseContext(m.session.getModelMetadata())
        case s: SignatureDefRequestContext =>
          SignatureDefResponseContext(s.session.getSignatureDef(s.name))
        case i: InputTensorInfoRequestContext =>
          InputTensorInfoResponseContext(
              i.session.getInputTensorInfo(i.signatureName, i.inputName))
        case p: PredictRequestContext[_] =>
          PredictResponseContext(predict(p.session, p.message), p.base64)
        case _ =>
          throw new RuntimeException(
              s"not supported request context: $requestContext")
      }
    } match {
      case Success(responseContext) =>
        logger.info("operate: "
            + requestContext.getClass().getSimpleName()
            + "(" + requestContext.session +")")
        responseContext
      case Failure(e) =>
        e match {
          case _: BadArgument =>
            logger.info(s"operate: '$e'")
          case _: InternalError =>
            logger.error(s"operate: internal error, $e", e)
          case _ =>
            logger.error(s"operate: unexpected error, $e", e)
        }
//        logger.info(s"$requestContext")
        throw e
    }
  }

  private def predict[T](session: ModelSession,
      message: T): PredictResponse = {
    message match {
      case request: PredictRequest =>
        session.predict(request)
      case params: PredictParams[_] =>
        session.predict(params.signatureName, params.tensorableContents)
      case _ =>
        throw new RuntimeException("not supported predict message: "
            + message.getClass().getName())
    }
  }
}
