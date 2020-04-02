package ksb.csle.component.operator.analysis

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.UserDefinedFunction

import com.google.protobuf.ByteString

import org.tensorflow.framework._
import org.tensorflow.framework.TensorShapeProto._
import org.tensorflow.framework.DataType._

import tensorflow.serving.Predict.PredictRequest
import tensorflow.serving.Model.ModelSpec

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.utils.tensorflow._

import TensorflowPredictOperator._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs the prediction according to a given tensorflow model.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.TensorflowPredictorInfo]]
 *          TensorflowPredictorInfo contains attributes as follows:
 *          - modelServerUri: Model location (required)
 *          - modelName: Model name (required)
 *          - signatureName: Signature name (required)
 *          - options: options of tensorflow prediction (repeated)
 *
 * ==TensorflowPredictorInfo==
 * {{{
 * message TensorflowPredictorInfo {
 *   required string modelServerUri = 1 [default = "http://ip:port"];
 *   required string modelName = 2 [default = "target_model_name"];
 *   required string signatureName = 3 [default = "target_signature_name"];
 *   repeated PredictOption options = 4;
 * }
 * }}}
 */
class TensorflowPredictOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val modelServerUri = o.getTfPredictor.getModelServerUri
  private[this] val modelName = o.getTfPredictor.getModelName
  private[this] val signatureName = o.getTfPredictor.getSignatureName
  private[this] val options = o.getTfPredictor.getOptionsList.map { o =>
    o.getKey -> o.getValue
  }.toMap

  private[this] val predictFunc: Row => Option[PredictionResult] = {
    row =>
      val client = ClientPool.get(modelServerUri, modelName, options)
      val signatureDef = client.getSignatureDef(signatureName)

      val builder = new RequestMessageBuilder(signatureDef,
          modelName, signatureName)
      Try {
        client.predict(builder.build(row))
      } match {
        case Success(rsp) =>
          val outputs = rsp.getOutputsMap().map { case (name, tensorProto) =>
            (name, toTensorRow(name, tensorProto))
          }
          Some(PredictionResult(outputs = outputs.toMap))
        case Failure(e) =>
          logger.info("predict error", e)
          Some(PredictionResult(error = e.getMessage()))
      }
  }

  /**
   * Operate the tensorflow model prediction.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has prediction output columns.
   */
  override def operate(inputs: DataFrame): DataFrame = {
    val client = ClientPool.get(modelServerUri, modelName, options)
    val signatureDef = client.getSignatureDef(signatureName)

    // append all outputs to '_tfp_outputs_' column.
    val udfPredict = udf(predictFunc)
    val allOutputs = inputs.withColumn(
        CN_TFP_OUTPUTS,
        udfPredict(struct(inputs.columns.map(inputs(_)) : _*)))

    // append each output to each column.
    val outUdf = signatureDef.getOutputsMap.map { case (name, info) =>
      buildOutUdf(name, info)
    }
    val eachOutput = outUdf.foldLeft(allOutputs) { case (df, (name, udf)) =>
      df.withColumn(name, udf(lit(name), allOutputs(CN_TFP_OUTPUTS)))
    }

    // drop the temporal column.
    eachOutput.drop(CN_TFP_OUTPUTS)
  }

  /**
   * Close the client connection with model server.
   */
  override def stop: Unit = {
    ClientPool.remove(modelServerUri)
  }
}

object TensorflowPredictOperator {
  import TensorUtil._

  private val CN_TFP_OUTPUTS = "_tfp_outputs_"

  private object ClientPool {
    import java.util.concurrent.ConcurrentHashMap

    private[this] val pool = new ConcurrentHashMap[String, PredictClient]()

    sys.addShutdownHook(shutdown)

//    def add(info: TensorflowPredictorInfo): PredictClient = {
//      val key = info.getModelServerUri
//      var client = pool.get(key)
//      if (client == null) {
//        client = PredictClientFactory.create(info.getModelServerUri(),
//            info.getModelName())
//        pool.putIfAbsent(key, client)
//      }
//      client
//    }

    def get(modelServerUri: String, modelName: String,
        options: Map[String, String]): PredictClient = {
      val key = modelServerUri
      var client = pool.get(key)
      if (client == null) {
        client = PredictClientFactory.create(modelServerUri, modelName,
            options)
        pool.putIfAbsent(key, client)
      }
      client
    }

    def remove(modelServerUri: String) {
      val key = modelServerUri
      val client = pool.remove(key)
      if (client != null) {
        client.shutdown()
      }
    }

    def shutdown() {
      pool.values().foreach(_.shutdown())
      pool.clear()
    }
  }

  private class RequestMessageBuilder(signatureDef: SignatureDef,
      modelName: String, signaturename: String) {

    def build(row: Row): PredictRequest = {
      val builder = PredictRequest.newBuilder()

      row.schema.fields.zipWithIndex.foreach { case (f, i) =>
        val info = signatureDef.getInputsMap.get(f.name)
        if (info != null) {
          builder.putInputs(f.name, toTensorProto(row, i, info))
        }
      }

      builder.setModelSpec(ModelSpec.newBuilder()
          .setName(modelName)
          .setSignatureName(signaturename))
      builder.build()
    }
  }

  private def toTensorProto(row: Row, index: Int,
      info: TensorInfo): TensorProto = {
    val builder = TensorProto.newBuilder()

    val numElements = row.schema.fields(index).dataType.typeName match {
      case "vector" =>
        addTensorVal(builder, info.getDtype, row.getAs[Vector](index))
      case _ =>
        addTensorVal(builder, info.getDtype, row, index)
    }

    builder.setDtype(info.getDtype)
    builder.setTensorShape(buildShapeProto(info.getTensorShape, numElements))
    builder.build()
  }

  private def addTensorVal(builder: TensorProto.Builder, dtype: DataType,
      vector: Vector): Int = {
    dtype match {
      case DT_FLOAT | DataType.DT_FLOAT_REF =>
        vector.toArray.foreach { d =>
          builder.addFloatVal(d.toFloat)
        }
      case DT_DOUBLE | DT_DOUBLE_REF =>
        vector.toArray.foreach { d =>
          builder.addDoubleVal(d)
        }
      case DT_INT32 | DT_INT16 | DT_INT8 | DT_UINT8
          | DT_INT32_REF | DT_INT16_REF | DT_INT8_REF | DT_UINT8_REF =>
        vector.toArray.foreach { d =>
          builder.addIntVal(d.toInt)
        }
      case DT_STRING | DT_STRING_REF =>
        throw new RuntimeException("can't convert to string type")
      case DT_INT64 | DT_INT64_REF =>
        vector.toArray.foreach { d =>
          builder.addInt64Val(d.toLong)
        }
      case DT_BOOL | DT_BOOL_REF =>
        throw new RuntimeException("can't convert to bool type")
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '${dtype.name()}'")
    }
    vector.size
  }

  private def addTensorVal(builder: TensorProto.Builder, dtype: DataType,
      row: Row, index: Int): Int = {
    dtype match {
      case DT_FLOAT | DT_FLOAT_REF =>
        addFloatTensorVal(builder, row, index)
      case DT_DOUBLE | DT_DOUBLE_REF =>
        addDoubleTensorVal(builder, row, index)
      case DT_INT32 | DT_INT16 | DT_INT8 | DT_UINT8
          | DT_INT32_REF | DT_INT16_REF | DT_INT8_REF | DT_UINT8_REF =>
        addIntTensorVal(builder, row, index)
      case DT_STRING | DT_STRING_REF =>
        addStringTensorVal(builder, row, index)
      case DT_INT64 | DT_INT64_REF =>
        addInt64TensorVal(builder, row, index)
      case DT_BOOL | DT_BOOL_REF =>
        addBoolTensorVal(builder, row, index)
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '${dtype.name()}'")
    }
  }

  private def addFloatTensorVal(builder: TensorProto.Builder, row: Row,
      index: Int): Int = {
    import org.apache.spark.sql.types._

    val ftype = row.schema.fields(index).dataType
    ftype match {
      case ArrayType(FloatType, _) =>
        builder.addAllFloatVal(row.getSeq[java.lang.Float](index))
      case ArrayType(DoubleType, _) =>
        row.getSeq[java.lang.Double](index).foreach { d =>
          builder.addFloatVal(d.toFloat)
        }
      case ArrayType(IntegerType, _) =>
        row.getSeq[java.lang.Integer](index).foreach { i =>
          builder.addFloatVal(i.toFloat)
        }
      case ArrayType(LongType, _) =>
        row.getSeq[java.lang.Long](index).foreach { l =>
          builder.addFloatVal(l.toFloat)
        }
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '$ftype'")
    }

    builder.getFloatValCount()
  }

  private def addDoubleTensorVal(builder: TensorProto.Builder, row: Row,
      index: Int): Int = {
    import org.apache.spark.sql.types._

    val ftype = row.schema.fields(index).dataType
    ftype match {
      case ArrayType(FloatType, _) =>
        row.getSeq[java.lang.Float](index).foreach { f =>
          builder.addDoubleVal(f.toDouble)
        }
      case ArrayType(DoubleType, _) =>
        builder.addAllDoubleVal(row.getSeq[java.lang.Double](index))
      case ArrayType(IntegerType, _) =>
        row.getSeq[java.lang.Integer](index).foreach { i =>
          builder.addDoubleVal(i.toDouble)
        }
      case ArrayType(LongType, _) =>
        row.getSeq[java.lang.Long](index).foreach { l =>
          builder.addDoubleVal(l.toDouble)
        }
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '$ftype'")
    }

    builder.getDoubleValCount()
  }

  private def addIntTensorVal(builder: TensorProto.Builder, row: Row,
      index: Int): Int = {
    import org.apache.spark.sql.types._

    val ftype = row.schema.fields(index).dataType
    ftype match {
      case ArrayType(FloatType, _) =>
        row.getSeq[java.lang.Float](index).foreach { f =>
          builder.addIntVal(f.toInt)
        }
      case ArrayType(DoubleType, _) =>
        row.getSeq[java.lang.Double](index).foreach { d =>
          builder.addIntVal(d.toInt)
        }
      case ArrayType(IntegerType, _) =>
        builder.addAllIntVal(row.getSeq[java.lang.Integer](index))
      case ArrayType(LongType, _) =>
        row.getSeq[java.lang.Long](index).foreach { l =>
          builder.addIntVal(l.toInt)
        }
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '$ftype'")
    }

    builder.getIntValCount()
  }

  private def addStringTensorVal(builder: TensorProto.Builder, row: Row,
      index: Int): Int = {
    import org.apache.spark.sql.types._

    val ftype = row.schema.fields(index).dataType
    ftype match {
      case ArrayType(ByteType, _) =>
        val bytes = row.getSeq[Byte](index).toArray
        builder.addStringVal(ByteString.copyFrom(bytes))
      case ArrayType(StringType, _) =>
        val text = row.getString(index)
        builder.addStringVal(ByteString.copyFromUtf8(text))
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '$ftype'")
    }

    builder.getStringValCount()
  }

  private def addInt64TensorVal(builder: TensorProto.Builder, row: Row,
      index: Int): Int = {
    import org.apache.spark.sql.types._

    val ftype = row.schema.fields(index).dataType
    ftype match {
      case ArrayType(FloatType, _) =>
        row.getSeq[java.lang.Float](index).foreach { f =>
          builder.addInt64Val(f.toLong)
        }
      case ArrayType(DoubleType, _) =>
        row.getSeq[java.lang.Double](index).foreach { d =>
          builder.addInt64Val(d.toLong)
        }
      case ArrayType(IntegerType, _) =>
        row.getSeq[java.lang.Integer](index).foreach { i =>
          builder.addInt64Val(i.toLong)
        }
      case ArrayType(LongType, _) =>
        builder.addAllInt64Val(row.getSeq[java.lang.Long](index))
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '$ftype'")
    }

    builder.getInt64ValCount()
  }

  private def addBoolTensorVal(builder: TensorProto.Builder, row: Row,
      index: Int): Int = {
    import org.apache.spark.sql.types._

    val ftype = row.schema.fields(index).dataType
    ftype match {
      case ArrayType(BooleanType, _) =>
        builder.addAllBoolVal(row.getSeq[java.lang.Boolean](index))
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '$ftype'")
    }

    builder.getBoolValCount()
  }

  case class PredictionResult(
      outputs: Map[String, TensorRow] = null, error: String = null)

  object PredictionResult {
    val OUTPUTS_INDEX = 0
    val ERROR_INDEX = 1
  }

  case class TensorRow(
      name: String, dataType: Int, shape: Seq[Long],
      floatVal: Seq[Float] = null,
      doubleVal: Seq[Double] = null,
      intVal: Seq[Int] = null,
      stringVal: Seq[Array[Byte]] = null,
      longVal: Seq[Long] = null,
      boolVal: Seq[Boolean] = null)

  object TensorRow {
    val NAME_INDEX = 0
    val DATATYPE_INDEX = 1
    val SHAPE_INDEX = 2
    val FLOATVAL_INDEX = 3
    val DOUBLEVAL_INDEX = 4
    val INTVAL_INDEX = 5
    val STRINGVAL_INDEX = 6
    val LONGVAL_INDEX = 7
    val BOOLVAL_INDEX = 8
  }

  private def toTensorRow(name: String, t: TensorProto): TensorRow = {
    val dataType = t.getDtypeValue

    t.getDtype match {
      case DT_FLOAT | DataType.DT_FLOAT_REF =>
        TensorRow(name, dataType,
            shape = toShape(t.getTensorShape, t.getFloatValCount),
            floatVal = t.getFloatValList().map(_.toFloat))
      case DT_DOUBLE | DT_DOUBLE_REF =>
        TensorRow(name, dataType,
            shape = toShape(t.getTensorShape, t.getDoubleValCount),
            doubleVal = t.getDoubleValList().map(_.toDouble))
      case DT_INT32 | DT_INT16 | DT_INT8 | DT_UINT8
          | DT_INT32_REF | DT_INT16_REF | DT_INT8_REF | DT_UINT8_REF =>
        TensorRow(name, dataType,
            shape = toShape(t.getTensorShape, t.getIntValCount),
            intVal = t.getIntValList().map(_.toInt))
      case DT_STRING | DT_STRING_REF =>
        TensorRow(name, dataType,
            shape = toShape(t.getTensorShape, t.getStringValCount),
            stringVal = t.getStringValList.map(_.toByteArray()))
      case DT_INT64 | DT_INT64_REF =>
        TensorRow(name, dataType,
            shape = toShape(t.getTensorShape, t.getInt64ValCount),
            longVal = t.getInt64ValList().map(_.toLong))
      case DT_BOOL | DT_BOOL_REF =>
        TensorRow(name, dataType,
            shape = toShape(t.getTensorShape, t.getBoolValCount),
            boolVal = t.getBoolValList().map(_.booleanValue()))
      case _ =>
        throw new RuntimeException(
            s"not supported data type: '${t.getDtype.name()}'")
    }
  }

  private def toShape(shapeProto: TensorShapeProto,
      count: Int): Seq[Long] = {
    if (shapeProto.getUnknownRank() || shapeProto.getDimList().isEmpty()) {
      Seq(0)
    } else {
      val product = shapeProto.getDimList.map(_.getSize).reduceLeft(_ * _)
      if (product < 0) {
        val noneDimSize = count / Math.abs(product)
        shapeProto.getDimList.map { d =>
          if (d.getSize < 0) {
            noneDimSize
          } else {
            d.getSize
          }
        }
      } else {
        shapeProto.getDimList.map(_.getSize)
      }
    }
  }

  private def getOutputs(row: Row): scala.collection.Map[String, Row] = {
    row.getString(PredictionResult.ERROR_INDEX) match {
      case null => row.getMap[String, Row](PredictionResult.OUTPUTS_INDEX)
      case _ => null
    }
  }

  private val toFloatVal: (String, Row) => Option[Seq[Float]] = {
    (name, row) =>
      val outputs = getOutputs(row)
      if (outputs == null) {
        None
      } else {
        outputs.get(name) match {
          case Some(r) => Some(r.getSeq[Float](TensorRow.FLOATVAL_INDEX))
          case None => None
        }
      }
  }

  private val toDoubleVal: (String, Row) => Option[Seq[Double]] = {
    (name, row) =>
      val outputs = getOutputs(row)
      if (outputs == null) {
        None
      } else {
        outputs.get(name) match {
          case Some(r) => Some(r.getSeq[Double](TensorRow.DOUBLEVAL_INDEX))
          case None => None
        }
      }
  }

  private val toIntVal: (String, Row) => Option[Seq[Int]] = {
    (name, row) =>
      val outputs = getOutputs(row)
      if (outputs == null) {
        None
      } else {
        outputs.get(name) match {
          case Some(r) => Some(r.getSeq[Int](TensorRow.INTVAL_INDEX))
          case None => None
        }
      }
  }

  private val toStringVal: (String, Row) => Option[Seq[Array[Byte]]] = {
    (name, row) =>
      val outputs = getOutputs(row)
      if (outputs == null) {
        None
      } else {
        outputs.get(name) match {
          case Some(r) =>
            Some(r.getSeq[Array[Byte]](TensorRow.STRINGVAL_INDEX))
          case None =>
            None
        }
      }
  }

  private val toLongVal: (String, Row) => Option[Seq[Long]] = {
    (name, row) =>
      val outputs = getOutputs(row)
      if (outputs == null) {
        None
      } else {
        outputs.get(name) match {
          case Some(r) => Some(r.getSeq[Long](TensorRow.LONGVAL_INDEX))
          case None => None
        }
      }
  }

  private val toBoolVal: (String, Row) => Option[Seq[Boolean]] = {
    (name, row) =>
      val outputs = getOutputs(row)
      if (outputs == null) {
        None
      } else {
        outputs.get(name) match {
          case Some(r) => Some(r.getSeq[Boolean](TensorRow.BOOLVAL_INDEX))
          case None => None
        }
      }
  }

  private def buildOutUdf(name: String, info: TensorInfo)
    : (String, UserDefinedFunction) = {
    info.getDtype match {
      case DT_FLOAT | DataType.DT_FLOAT_REF =>
        (name, udf(toFloatVal))
      case DT_DOUBLE | DT_DOUBLE_REF =>
        (name, udf(toDoubleVal))
      case DT_INT32 | DT_INT16 | DT_INT8 | DT_UINT8
          | DT_INT32_REF | DT_INT16_REF | DT_INT8_REF | DT_UINT8_REF =>
        (name, udf(toIntVal))
      case DT_STRING | DT_STRING_REF =>
        (name, udf(toStringVal))
      case DT_INT64 | DT_INT64_REF =>
        (name, udf(toLongVal))
      case DT_BOOL | DT_BOOL_REF =>
        (name, udf(toBoolVal))
      case _ =>
        throw new RuntimeException(
            s"no match udf: '${info.getDtype.name()}'")
    }
  }
}
