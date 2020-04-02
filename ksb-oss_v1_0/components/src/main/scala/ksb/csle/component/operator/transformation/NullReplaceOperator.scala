package ksb.csle.component.operator.transformation

import java.sql.Timestamp
import java.lang.String
import scala.collection.JavaConversions._

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto.AsNullReplaceOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that replaces empty string and 0 value with null value.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AsNullReplaceOperatorInfo]]
 *          AsNullReplaceOperatorInfo contains attributes as follows:
 *          - EmpytStringAsNullReplace: Parameter to specify whether to replace
 *                             empty string with null value or not (required)
 *          - ZeroAsNullReplace: Parameter to specify whether to replace zero
 *                             value with null value or not (required)
 *
 *  ==AsNullReplaceOperatorInfo==
 *  {{{
 *  message AsNullReplaceOperatorInfo {
 *  required bool EmpytStringAsNullReplace = 1 [default = true];
 *  required bool ZeroAsNullReplace = 2 [default = false];
 *  }
 *  }}}
 */
class NullReplaceOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: AsNullReplaceOperatorInfo = o.getAsNullReplace

  // Replaces empty Strings with null values
  // Replaces 0 with null values
  private def emptyToNull(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : AsNullReplacer")
    logger.info("Input dataframe :" + Utils.printNColumns(df,10).show.toString)
    logger.info(df.printSchema().toString)

    val exprs = df.schema.map { f =>
      f.dataType match {
        case StringType =>
          if (p.getEmpytStringAsNullReplace)
            when(length(col(f.name)) === 0, lit(null: String).cast(StringType))
            .otherwise(col(f.name)).as(f.name)
          else
            col(f.name)
        case TimestampType => col(f.name)
        case IntegerType =>
          if (p.getZeroAsNullReplace)
            when(col(f.name) === 0, lit(null: String).cast(IntegerType))
            .otherwise(col(f.name)).as(f.name)
          else
            col(f.name)
        case DoubleType | IntegerType | FloatType | LongType =>
          if (p.getZeroAsNullReplace)
            when(col(f.name) === 0, lit(null: String).cast(DoubleType))
            .otherwise(col(f.name)).as(f.name)
          else
            col(f.name)
        case _ => col(f.name)
      }
    }
    val result = df.select(exprs: _*)
    logger.info(
        "Output dataframe :" + Utils.printNColumns(result,10).show.toString)
    logger.info(result.printSchema().toString)
    result
  }

  /**
   * Operates AsNullReplacer function using following params.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = emptyToNull(df)
}

object NullReplaceOperator {
  def apply(o: StreamOperatorInfo): NullReplaceOperator =
    new NullReplaceOperator(o)
}