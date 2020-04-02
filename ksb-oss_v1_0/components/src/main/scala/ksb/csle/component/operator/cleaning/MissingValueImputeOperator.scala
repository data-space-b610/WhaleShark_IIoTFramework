package ksb.csle.component.operator.cleaning

import scala.collection.JavaConversions._

import com.google.protobuf.Message
import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto.MissingValueImputationInfo
import ksb.csle.common.proto.StreamOperatorProto.MissingValueImputationInfo.Scope
import ksb.csle.common.proto.StreamOperatorProto.MissingValueImputationInfo.How
import ksb.csle.common.proto.StreamOperatorProto.MissingValueImputationInfo.Method
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that imputes the missing values of a given dataframe according to
 * the given condition.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.MissingValueImputationInfo]]
 *          MissingValueImputationInfo contains attributes as follows:
 *          - scope: Scope to apply a missing value imputation function.
 *                   Enum(SCOPE_ALL, SCOPE_SELECTED) (required)
 *          - selectedColumnId: Column Ids to be selected (repeated)
 *          - method: Method of MVI. Enum(IM, SPECIFIC_VALUE, MEDIAN, MEAN) (required)
 *          - how: Drop condition. Enum(HOW_ANY, HOW_ALL) (required)
 *                'HOW_ANY': Rows containing any null values are dropped.
 *                'HOW_ALL': Rows only if every column is null are dropped.
 *          - subParam: Option to fill a specific value {numeric, string}
 *                      (repeated)
 *
 *  ==MissingValueImputationInfo==
 *  {{{
 *  message MissingValueImputationInfo {
 *  required Scope scope = 4 [default = SCOPE_SELECTED];
 *  repeated int32 selectedColumnId = 5;
 *  required Method method = 6 [default = SPECIFIC_VALUE];
 *  required How how = 7 [default = HOW_ANY]; // for IM
 *  repeated SubParameter subParam = 8 ; // for method SPECIFIC_VALUE
 *  enum Scope {
 *    SCOPE_ALL = 0;
 *    SCOPE_SELECTED = 1;
 *  }
 *  enum Method {
 *    IM = 0;
 *    SPECIFIC_VALUE = 1;
 *    MEDIAN = 2;
 *    MEAN = 3;
 *  }
 *  enum How {
 *    HOW_ANY = 0;
 *    HOW_ALL = 1;
 *  }
 *  }
 *  }}}
 */
class MissingValueImputeOperator(
    o: StreamOperatorInfo) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: MissingValueImputationInfo = o.getMissingValueImputation
  /**
   * Runs missingValueImputation operation using following params.
   */
  private def missingValueImputation(df: DataFrame): DataFrame = {
    // Select a scope the MVI is applied.
    val result = p.getScope match {
      case Scope.SCOPE_ALL => MVI_ALL(df)
      case Scope.SCOPE_SELECTED => MVI_SELECTED(df)
      case _ =>
        throw new RuntimeException("Check Scope")
    }
    logger.info(s"OpId ${o.getId} : missingValueImputation")
    val selectedDf = Utils.printNColumns(result,10)
    logger.info("Output dataframe :" + selectedDf.show.toString)
//    logger.info("Output dataframe :" + result.show.toString)
//    Utils.printDEBUG(result)
    result
  }

  /**
   * Applies the MVI to the whole dataframe.
   */
  private def MVI_ALL(src: DataFrame): DataFrame = {
    var result = p.getMethod match {
      case Method.IM => MVI_IM_ALL(src)
      case Method.SPECIFIC_VALUE => MVI_SpecificValue_ALL(src)
      case Method.MEDIAN => MVI_Median_ALL(src)
      case Method.MEAN => MVI_Mean_ALL(src)
      case _ => MVI_SpecificValue_ALL(src)
    }
    result
  }

  /**
   * Applies the MVI to the selected columns.
   */
  private def MVI_SELECTED(src: DataFrame): DataFrame = {
    val result = p.getMethod match {
      case Method.IM => MVI_IM_SELECTED(src)
      case Method.SPECIFIC_VALUE => MVI_SpecificValue_SELECTED(src)
      case Method.MEDIAN => MVI_Median_SELECTED(src)
      case Method.MEAN => MVI_Mean_SELECTED(src)
      case _ => MVI_SpecificValue_SELECTED(src)
    }
    result
  }

  /**
   *  Drops rows containing any null values in case of 'HOW_ANY'.
   *  Drops rows only if every column is null in case of 'HOW_ALL'.
   */
  private def MVI_IM_ALL(src: DataFrame): DataFrame = {
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    val how = p.getHow match {
      case How.HOW_ANY => "any"
      case How.HOW_ALL => "all"
    }
    var result = src.na.drop(how)
    result
  }

  /**
   *  Drops rows containing any null values in the specified columns (HOW_ANY).
   *  Drops rows only if every specified column is null for that row (HOW_ALL).
   */
  private def MVI_IM_SELECTED(src: DataFrame): DataFrame  = {
    val columnNames: Array[String] =
      Utils.getColumnNames(p.getSelectedColumnIdList, src)
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    var result = p.getHow match {
      case How.HOW_ANY => src.na.drop("any", columnNames)
      case How.HOW_ALL => src.na.drop(columnNames)
    }
    result
  }

  /**
   *  Replaces null values in numeric columns and string columns with value.
   */
  private def MVI_SpecificValue_ALL(src: DataFrame): DataFrame = {
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    var result = src
    if (options.contains("numeric")) {
      result = result.na.fill(options.get("numeric").get.toDouble)
    }
    if (options.contains("string")) {
      result = result.na.fill(options.get("string").get)
    }
    result
  }

  /**
   *  Replaces null values in specified numeric columns and string columns
   *  with value.
   */
  private def MVI_SpecificValue_SELECTED(src: DataFrame): DataFrame = {
    val columnNames: Array[String] =
        Utils.getColumnNames(p.getSelectedColumnIdList, src)
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    var result = src
    if (options.contains("numeric")) {
      result = result.na.fill(options.get("numeric").get.toDouble, columnNames)
    }
    if (options.contains("string")) {
      result = result.na.fill(options.get("string").get, columnNames)
    }
    result
  }

  /**
   *  Replaces null values in numeric columns with median value.
   */
  private def MVI_Median_ALL(src: DataFrame): DataFrame = {
    val columnNames: Array[String] = src.columns
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    var fillValues: Map[String, Any] = null
    fillValues = MedianUtil.median(src, columnNames)
    var result = src.na.fill(fillValues)
    result
  }

  /**
   *  Replaces null values in specific numeric columns with median value.
   */
  private def MVI_Median_SELECTED(src: DataFrame): DataFrame = {
    val columnNames: Array[String] =
      Utils.getColumnNames(p.getSelectedColumnIdList, src)
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    var fillValues: Map[String, Any] = null
    fillValues = MedianUtil.median(src, columnNames)
    var result = src.na.fill(fillValues)
    result
  }

  /**
   *  Replaces null values in numeric columns with mean value.
   */
  private def MVI_Mean_ALL(src: DataFrame): DataFrame = {
    val columnNames: Array[String] = src.columns
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    var fillValues: Map[String, Any] = null
    fillValues = withSparkAggMethods(src, columnNames, "avg")
    var result = src.na.fill(fillValues)
    result
  }

  /**
   *  Replaces null values in numeric columns with mean value.
   */
  private def MVI_Mean_SELECTED(src: DataFrame): DataFrame = {
    val columnNames: Array[String] =
      Utils.getColumnNames(p.getSelectedColumnIdList, src)
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    var fillValues: Map[String, Any] = null
    fillValues = withSparkAggMethods(src, columnNames, "avg")
    var result = src.na.fill(fillValues)
    result
  }

  private def withSparkAggMethods(
      dataset: DataFrame,
      columnNames: Array[String],
      agg_method: String): Map[String, Any] = {
    val agg_method_checked = Utils.checkAggregateMethods(agg_method)
    val tempMap = scala.collection.mutable.Map[String, String]()
    columnNames.map(x => tempMap += x -> agg_method_checked)
    val tempDF: Dataset[Row] = dataset.agg(tempMap.toMap)
    val tempDF2 = tempDF.na.fill(Double.NaN)
    val tempColNames: Array[String] = tempDF2.columns.map(
      x => x.substring(x.indexOf("(")+1, x.length-1))
    tempColNames.zip(tempDF2.head.toSeq).toMap
  }

  /**
   * Operates the missing value imputation.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = missingValueImputation(df)
}

object MissingValueImputeOperator {
  def apply(o: StreamOperatorInfo) = new MissingValueImputeOperator(o)
}