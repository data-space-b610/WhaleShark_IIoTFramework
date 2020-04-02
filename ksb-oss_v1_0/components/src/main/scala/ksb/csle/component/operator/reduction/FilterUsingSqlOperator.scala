package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions._

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs to filter using SQL in the given dataframe.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.FilterUsingSqlInfo]]
 *          FilterUsingSqlInfo contains attributes as follows:
 *          - selectedColumnId: Column ids to be filterd by SQL (required)
 *          - subParam: Option to filter using SQL (repeated)
 *                      Ex: {"sql_where", "DATE_TIME < '2015_09_01_00_05'
 *                           ORDER BY DATE_TIME"}
 *
 *  ==FilterUsingSqlInfo==
 *  {{{
 *  message FilterUsingSqlInfo {
 *  required int32 selectedColumnId = 4;
 *  repeated SubParameter subParam = 5;
 *  }
 *  }}}
 */
class FilterUsingSqlOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o){

  val p: ksb.csle.common.proto.StreamOperatorProto.FilterUsingSqlInfo =
    o.getFilterUsingSql

  private def filterUsingSql(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : FilterUsingSql")
    val columnNames: String =
      Utils.getSingleColumnName(p.getSelectedColumnId, df)
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    val SQL_SELECT: String =
      if(options.contains("sql_select")) {
        options.get("sql_select").get
      } else {
        "*"
       }
    val SQL_WHERE: String =
      if(options.contains("sql_where")) {
        options.get("sql_where").get
      } else {
        throw new RuntimeException("""Check Option 'sql_where'""")
      }
    df.createOrReplaceTempView("data_temp")
    val SQL_QUERY: String =
      "SELECT " + SQL_SELECT + " FROM data_temp WHERE " + columnNames +
      SQL_WHERE + " " + columnNames
    logger.info(SQL_QUERY)
    val result = df.sparkSession.sql(SQL_QUERY)
    logger.info(result.show.toString)
    logger.info(result.printSchema.toString)
    result
  }

  /**
   * Operates filterUsingSql function using following params.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = filterUsingSql(df)
}

object FilterUsingSqlOperator {
  def apply(o: StreamOperatorInfo): FilterUsingSqlOperator =
    new FilterUsingSqlOperator(o)
}