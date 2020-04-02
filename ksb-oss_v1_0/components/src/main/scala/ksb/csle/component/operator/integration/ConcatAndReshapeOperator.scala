package ksb.csle.component.operator.integration

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamOperatorProto.ReshapeWithConcatInfo.Condition
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that reshapes with row concatenation in the given dataframe.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.ReshapeWithConcatInfo]]
 *          ReshapeWithConcatInfo contains attributes as follows:
 *          - selectedColumnId: Column ids which should be used for concatenation
 *                              (repeated)
 *          - delimiter: Delimiter to concatenate the given columns (required)
 *          - valueColName: Final column name after concatenation (required)
 *          - condition: Method to aggregate Enum(KEEP_ORIGINAL_AND_RESULT,
 *                       REPLACE_SELECTED_WITH_RESULT, REFINEMENT_RESULT_ONLY)
 *                       (required)
 *
 *  ==ReshapeWithConcatInfo==
 *  {{{
 *  message ReshapeWithConcatInfo {
 *  repeated int32 selectedColumnId = 4;
 *  required string delimiter = 5;
 *  required string valueColName = 6;
 *  required Condition condition = 7 [default = KEEP_ORIGINAL_AND_RESULT];
 *  enum Condition {
 *    KEEP_ORIGINAL_AND_RESULT = 0;
 *    REPLACE_SELECTED_WITH_RESULT = 1;
 *    REFINEMENT_RESULT_ONLY = 2;
 *  }
 *  }
 *  }}}
 */
class ConcatAndReshapeOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o){

  val p: ksb.csle.common.proto.StreamOperatorProto.ReshapeWithConcatInfo =
    o.getReshapeWithConcat

  private def reshapeWithConcat(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : ReshapeWithConcat")
//    logger.info("Input dataframe :" + df.show.toString)
    val columnNames: Array[String] =
      Utils.getColumnNames(p.getSelectedColumnIdList, df)
    var temp: Column = df.col(columnNames(0))
    for (i <- 1 until columnNames.length) {
      temp = concat(temp, lit(p.getDelimiter), df.col(columnNames(i)))
    }
    var result = df;
    p.getCondition match {
      case Condition.KEEP_ORIGINAL_AND_RESULT => {
        result = df.withColumn(p.getValueColName, temp)
      }
      case Condition.REPLACE_SELECTED_WITH_RESULT => {
        result = df.withColumn(p.getValueColName, temp)
          .drop(columnNames.mkString(","))
      }
      case Condition.REFINEMENT_RESULT_ONLY => {
        result = df.select(temp as p.getValueColName)
      }
    }
    logger.info("Output dataframe :" + result.show.toString)
//    logger.info(result.printSchema.toString)
    result
  }

  /**
   * Operates reshapeWithConcat.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame
         = reshapeWithConcat(df)

}
object ReshapeWithConcat {
  def apply(o: StreamOperatorInfo): ConcatAndReshapeOperator =
    new ConcatAndReshapeOperator(o)
}