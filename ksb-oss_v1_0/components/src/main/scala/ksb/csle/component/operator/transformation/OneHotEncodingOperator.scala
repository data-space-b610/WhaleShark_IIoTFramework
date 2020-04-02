package ksb.csle.component.operator.transformation

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.StringIndexer

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto.OneHotEncodingInfo
import ksb.csle.component.ingestion.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that replaces empty string and 0 value with null value.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.OneHotEncodingInfo]]
 *          OneHotEncodingInfo contains attributes as follows:
 *          - selectedColumnName: Column name to be selected (required)
 *
 *  ==OneHotEncodingInfo==
 *  {{{
 *  message OneHotEncodingInfo {
 *  required string selectedColumnName = 1;
 *  }
 *  }}}
 */
class OneHotEncodingOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.OneHotEncodingInfo =
    o.getOneHotEncoding

  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
      try {
        if(!df.columns.contains(p.getSelectedColumnName))
          throw new DataException("Column is not exist")
      } catch {
        case e: DataException => throw e
        case e: Exception     => throw new DataException(
            s"validate Error : ", e)
      }
      df
    }

  /**
   * Runs oneHotEncoding operation using following params.
   */
  @throws(classOf[KsbException])
  private def oneHotEncoding(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : OneHotEncoding")
    var result: DataFrame = null
    try {
      val indexer = new StringIndexer()
        .setInputCol(p.getSelectedColumnName)
        .setOutputCol(p.getSelectedColumnName + "Index")
        .fit(df)
      val indexed = indexer.transform(df)
      val encoder = new OneHotEncoder()
        .setInputCol(p.getSelectedColumnName + "Index")
        .setOutputCol(p.getSelectedColumnName + "Vec")
      result = encoder.transform(indexed)
      result.show
    } catch {
      case e: Exception => throw new ProcessException(
          s"OneHotEncoding Process Error : ", e)
    }
    result
  }

  /**
   * Operates OneHotEncoding.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = oneHotEncoding(df)
}

object OneHotEncodingOperator {
  def apply(o: StreamOperatorInfo): OneHotEncodingOperator =
    new OneHotEncodingOperator(o)
}