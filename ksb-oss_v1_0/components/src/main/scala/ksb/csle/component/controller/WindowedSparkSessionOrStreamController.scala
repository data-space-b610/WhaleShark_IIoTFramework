package ksb.csle.component.controller

import java.lang.reflect.ParameterizedType

import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.utils._
import ksb.csle.common.base.{Doer, StaticDoer}
import ksb.csle.common.base.controller.BaseGenericController
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.controller.BaseController
import ksb.csle.common.base.result.DefaultResult
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode, Row}

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that conduct windowing control to queuing data.
 * This controller passes queued data to a consecutive operators.
 * This can be run on streaming engine,
 * and work as a streaming processing controller.
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
final class WindowedSparkSessionOrStreamController[T, P, R, SI, SO](
    override val runner: BaseRunner[SI, _, _],
    override val i: BaseReader[T, _, SI],
    override val o: BaseWriter[R, _, SO],
    override val p: StreamControllerInfo,
    override val ops: List[BaseGenericOperator[ _, R]]
    ) extends BaseGenericController[T, StreamControllerInfo, R, SI, SO](runner, i, o, p, ops) {

  val ssIn: SparkSession = runner.getSession.asInstanceOf[SparkSession]
  val ssOut: SparkSession = runner.getSession.asInstanceOf[SparkSession]
  val x = p.getWindowedSparkSessionOrStreamController
  logger.info("operationPeriod : " + x.getOperationPeriod)
  logger.info("Master : " + ssIn.sparkContext.master)
  val sscIn: StreamingContext = SparkUtils.getStreamingContext(ssIn,ssIn.sparkContext.master,x.getOperationPeriod)
  val sscOut: StreamingContext = SparkUtils.getStreamingContext(ssOut,ssOut.sparkContext.master,x.getOperationPeriod)

  var rowCountToSend: Int = 0
  var bufferCount: Int = 0
  var df : DataFrame = null
  var inputDf : DataFrame = null
  var resultDf : DataFrame = null
  var bufferArray : Array[Row] = Array()
  var tempArray4Engine : Array[Row] = Array()
  var tempArray4Predict : Array[Row] = Array()
  var newRows: Array[Row] = Array()
  //  val windowSize: Int = 10
  //  val inputQueSize:Int = 1
  val windowSize: Int = x.getWindowSize
  val inputQueSize:Int = x.getInputQueSize
  private val isStreaming =
    i.getClass.getGenericSuperclass.asInstanceOf[ParameterizedType].getTypeName match {
      case "org.apache.spark.streaming.StreamingContext" => true
      case _ => false
    }

  override def process(): BaseResult = {
    runner.init()
    execute(i, o)
    if (SparkUtils.hasOutputOperations(sscIn)) {
      SparkUtils.startStreaming(sscIn)
      logger.info("SparkStreamingContext started")
      SparkUtils.awaitStreamingTermination(sscIn)
    }
    DefaultResult("s", "p", "o").asInstanceOf[BaseResult]
  }

  private def execute(reader: BaseReader[T, _, _], writer: BaseWriter[R, _, _]) {
    val superClass = reader.getClass.getGenericSuperclass
    val sessionType = superClass.asInstanceOf[ParameterizedType].getActualTypeArguments()(2)
    val input = sessionType.getTypeName match {
      case "org.apache.spark.sql.SparkSession" =>
        reader.asInstanceOf[BaseReader[T, _, SparkSession]].read(ssIn)
      case "org.apache.spark.streaming.StreamingContext" =>
        reader.asInstanceOf[BaseReader[T, _, StreamingContext]].read(sscIn)
      case _ =>
        throw new RuntimeException("Not supported session type.")
    }

    input match {
      case df: Dataset[_] =>
        writer.write(pipeOperators(runner, ops, df.asInstanceOf[R]))
      case dstream: InputDStream[_] =>
        dstream.foreachRDD { rdd =>
          if (rdd.isEmpty() == false) {
            val jsonRDD = rdd.map { e =>
            e match {
              case json: String => json
              case record: ConsumerRecord[_, _] => record.value().toString()
              case _ => e.toString() }}
            val df = ssIn.sqlContext.read.json(jsonRDD)

            logger.info("Input dataframe :" + df.show.toString)
            newRows = df.rdd.collect()
            bufferCount = bufferCount + newRows.size
            bufferArray ++=newRows
            logger.info("new input data row count :" + newRows.size)
            logger.info("contained input data count in buffer:" + bufferCount)
            //Operates data engine when the accumulated que size satisfies the inputQueSize.
            if(bufferCount >= inputQueSize) {
              tempArray4Engine = bufferArray.take(inputQueSize)
              bufferArray = bufferArray.drop(inputQueSize)
              bufferCount = bufferCount - inputQueSize
              inputDf = ssIn.sqlContext.createDataFrame(tempArray4Engine.toList,df.schema)
              newRows = inputDf.rdd.collect()
              rowCountToSend = rowCountToSend + newRows.size
              logger.info("total number of input data of predict buffer to send to the predict operator : " + rowCountToSend)
              tempArray4Predict++= newRows

              if(rowCountToSend > windowSize-1) {
                val sizeColumns = inputDf.columns.length
                val columnNames: Array[String] =
                Array.range(0,sizeColumns).map(x => inputDf.columns(x))
                resultDf = ssIn.sqlContext.createDataFrame(tempArray4Predict.toList,inputDf.schema)
                //Control Time Window Que to send by kafka
                if(tempArray4Predict.size >= windowSize)
                  tempArray4Predict = tempArray4Predict.drop(
                      tempArray4Predict.size-windowSize+1)
                val res = pipeOperators(runner, ops, resultDf.asInstanceOf[R]).asInstanceOf[DataFrame]
                    o.write(res.asInstanceOf[R])
              }
              logger.info("contained data count in predict buffer :" + tempArray4Predict.size)
              logger.info(" ")
              logger.info(" ")
            }
          }
        }
      }
  }

  override def stop: Any = {
    sscIn.getState() match {
      case StreamingContextState.ACTIVE =>
        try {
          sscIn.stop()
        } catch {
          case e: Exception =>
            logger.warn(s"SparkStreamingContext stop error: $e")
        }
      case _ =>
        // do nothing, streaming is not started.
    }
    super.stop()
  }
}
