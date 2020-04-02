package ksb.csle.component.operator.transformation

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.functions.{array, lit, map, struct}

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.TransposeInfo
import ksb.csle.component.operator.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that converts the dataframe with single column to the dataframe
 * with one row. It can be used to transpose one column to one row.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.TransposeInfo]]
 *          TransposeInfo contains attributes as follows:
 *          - selectedColumnName: Column name to be selected. (required)
 *
 *  ==TransposeInfo==
 *  {{{
 *  message TransposeInfo {
 *  required string selectedColumnName = 3;
 *  }
 *  }}}
 */
class TransposeOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.TransposeInfo =
    o.getTranspose

  @throws(classOf[KsbException])
  private def transpose(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : Transpose")
//    logger.info(df.show.toString)
//    logger.info(df.printSchema.toString)
    var result = df
    try {
      val dfTemp = df.withColumn(
          "newCol", lit(1)).withColumn("id",monotonicallyIncreasingId)
      result = dfTemp.groupBy("newCol").pivot("id")
      .sum(p.getSelectedColumnName).sort(dfTemp.col("newCol")).drop("newCol")
    } catch {
      case e: Exception => throw new ProcessException(
          s"transpose Process Error : ", e)
    }
    val selectedDf = Utils.printNColumns(result,10)
    logger.info("Output dataframe :" + selectedDf.show.toString)
    result
  }

  /**
   * Operates transpose function.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   *
   */
  override def operate(df: DataFrame): DataFrame = transpose(df)
}

object TransposeOperator {
  def apply(o: StreamOperatorInfo): TransposeOperator =
    new TransposeOperator(o)
}