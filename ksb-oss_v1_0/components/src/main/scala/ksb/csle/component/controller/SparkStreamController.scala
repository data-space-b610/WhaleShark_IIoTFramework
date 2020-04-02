package ksb.csle.component.controller

import java.lang.reflect.ParameterizedType

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import org.apache.kafka.clients.consumer.ConsumerRecord

import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.controller.BaseGenericController
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.result._
import ksb.csle.common.proto.WorkflowProto.StreamControllerInfo
import ksb.csle.common.utils.SparkUtils

import ksb.csle.component.ingestion.util.BufferUtils.WindowedBuffer

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that simply pipelines DataFrame from reader to writer
 * via a series of operators.
 * This controller can be run on streaming engine
 * and work as streaming processing controller.
 *
 * @tparam T     Data type class to be passes through data pipeline
 *               from reader to operators
 * @tparam R     Data type for the final data format
 * @tparam P     Message type class
 * @tparam SI    Session type class for reader object
 * @tparam SO    Session type class for writer object
 * @param runner Runner object that runs spark session
 * @param i      Reader object that reads data from data source
 * @param o      Writer object that writes pipelined data from operators
 * @param p      Message object that has one of messages object containing
 *               attributes related to controlling process pipelining
 * @param ops    a list of [[BaseGenericOperator]] objects that processes data
 *               one by one.
 */
class SparkStreamController[U, SI](
    override val runner: BaseRunner[SI, _, _],
    override val i: BaseReader[InputDStream[U], _, SI],
    override val o: BaseWriter[DataFrame, _, SparkSession],
    override val p: StreamControllerInfo,
    override val ops: List[BaseGenericOperator[ _, DataFrame]]
    ) extends BaseGenericController[
      InputDStream[U], StreamControllerInfo, DataFrame,
      SI, SparkSession](runner, i, o, p, ops) {

  private[this] val info =
    if (p.getSparkStreamController == null) {
      p.getSparkStreamController.getDefaultInstanceForType
    } else {
      p.getSparkStreamController
    }

  private[this] val session = runner.getSession match {
    case ss: SparkSession => ss
    case _ => throw new IllegalArgumentException(
        s"not supported session type: ${runner.getSession}")
  }

  private[this] val context = SparkUtils.getStreamingContext(session,
      session.sparkContext.master, info.getOperationPeriod)

  private[this] val buffer =
    if (info.getWindowSize > 0 && info.getSlidingSize > 0) {
      new WindowedBuffer[String](info.getWindowSize,
          info.getSlidingSize)
    } else {
      null
    }

  /**
   * Pipelines data from reader to writer via a series of operators.
   */
  override def process(): BaseResult = {
    readStream(i).foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val jsonRDD = toJsonRDD(rdd)
        toDataFrameList(jsonRDD).foreach { df =>
          o.write(pipeOperators(runner, ops, df))
        }
      }
    }

    SparkUtils.startStreaming(context)
    SparkUtils.awaitStreamingTermination(context)

    DefaultResult("s", "p", "o").asInstanceOf[BaseResult]
  }

  private def readStream(
      reader: BaseReader[InputDStream[U], _, _]): InputDStream[U] = {
    val superClass = reader.getClass.getGenericSuperclass
    val sessionType = superClass.asInstanceOf[ParameterizedType]
      .getActualTypeArguments()(2)

    sessionType.getTypeName match {
      case "org.apache.spark.sql.SparkSession" =>
        reader.asInstanceOf[BaseReader[InputDStream[U], _, SparkSession]]
          .read(session)
      case "org.apache.spark.streaming.StreamingContext" =>
        reader.asInstanceOf[BaseReader[InputDStream[U], _, StreamingContext]]
          .read(context)
      case _ =>
        throw new RuntimeException(
            s"not supported session type: ${sessionType.getTypeName}")
    }
  }

  private def toJsonRDD(rdd: RDD[U]): RDD[String] = {
    rdd.map { e =>
      e match {
        case json: String => json
        case record: ConsumerRecord[_, _] => record.value().toString()
        case _ => e.toString()
      }
    }
  }

  private def toDataFrame(rdd: RDD[String]): DataFrame = {
    if (buffer == null) {
      session.sqlContext.read.json(rdd)
    } else {
      buffer.addAll(rdd.collect())
      val windowed = buffer.get()
      if (windowed.nonEmpty) {
        val windowedRDD = session.sparkContext.parallelize(windowed)
        val windowedDataFrame = session.sqlContext.read.json(windowedRDD)
        windowedDataFrame
      } else {
        null
      }
    }
  }

  private def toDataFrameList(rdd: RDD[String]): Seq[DataFrame] = {
    if (buffer == null) {
      Seq(session.sqlContext.read.json(rdd))
    } else {
      buffer.addAll(rdd.collect())

      val windowedList = buffer.getAll()
      if (windowedList.isEmpty) {
        Seq.empty
      } else {
        windowedList.map { e =>
          val rdd = session.sparkContext.parallelize(e)
          val df = session.sqlContext.read.json(rdd)
          df
        }
      }
    }
  }

  override def stop: Any = {
    context.stop()
    super.stop()
  }
}
