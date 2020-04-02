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
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that rescales the selected columns individually to a common range
 * [min, max]. If 'withMinMaxRange' is false, the max value in selected column
 * becomes the rescaled max value. If 'withMinMaxRange' is true, 'maxRealValue'
 * parameter becomes the rescaled max value.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.MinMaxScalingInfo]]
 *          MinMaxScalingInfo contains attributes as follows:
 *          - selectedColumnId: Column ids to be rescaled (repeated)
 *          - max: Max value of a range (required)
 *                 Max value in selected column becomes the rescaled max value.
 *          - min: Min value of a range (required)
 *                 Min value in selected column becomes the rescaled min value.
 *          - withMinMaxRange: Parameter to specify whether the specific max/min
 *                             value is used or not (required)
 *          - maxRealValue: Real max value (optional)
 *          - minRealValue: Real min value (optional)
 *
 *  ==MinMaxScalingInfo==
 *  {{{
 *  message MinMaxScalingInfo {
 *  repeated int32 selectedColumnId = 4;
 *  required string max = 5;
 *  required string min = 6;
 *  required bool withMinMaxRange = 7 [default = false];
 *  optional string maxRealValue = 8;
 *  optional string minRealValue = 9;
 *  }
 *  }}}
 */
class MinMaxScalingOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.MinMaxScalingInfo =
    o.getMinMaxScaling
  var tempDf : DataFrame = null
  var result : DataFrame = null
  var modDf : DataFrame = null
  private def minMaxScaling(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : MinMaxScaling")
    val columnNames: Array[String] =
        Utils.getColumnNames(p.getSelectedColumnIdList, df)
    modDf = df
    for (i <- 0 until columnNames.length) {
      val exist = modDf.schema.fieldNames.contains(columnNames(i))
      if(exist){
        if(modDf.schema(columnNames(i)).dataType != DoubleType){
          modDf = modDf.withColumn(
              columnNames(i), modDf.col(columnNames(i)).cast(DataTypes.DoubleType))
           }
      }else{
        logger.info("No columnName in the dataframe : " + columnNames(i))
      }
    }
    val assembler = new org.apache.spark.ml.feature.VectorAssembler()
      .setInputCols(columnNames)
      .setOutputCol("vector")
    val dfColLength = modDf.count.toInt
    //For normalization in a range. (minRealValue ~ maxRealValue)
    if(p.getWithMinMaxRange == true){
      val maxRealValue = p.getMaxRealValue.toDouble
      val minRealValue = p.getMinRealValue.toDouble
      val sp = df.sparkSession
      import sp.sqlContext.implicits._
      val schemaFieldNames = modDf.schema.fieldNames.toSeq
      var schemaFieldType =
        schemaFieldNames.map(x => modDf.schema(x).dataType)
      val columnLen = schemaFieldNames.length
      val tempRdd = sp.sparkContext.parallelize(modDf.take(1))
      tempDf = sp.sqlContext.createDataFrame(tempRdd, modDf.schema)
      val listD : java.util.List[Integer] = p.getSelectedColumnIdList
      val columnName = schemaFieldNames(listD(0))
      val tempDfmax = tempDf.withColumn(
          columnName, when(col(columnName).isNotNull, maxRealValue))
      val tempDfmin = tempDf.withColumn(
          columnName, when(col(columnName).isNotNull, minRealValue))
      tempDf= modDf.union(tempDfmax).union(tempDfmin)
    }else{
      tempDf= modDf
    }
    val df2 = assembler.transform(tempDf)
    val maxValue = p.getMax.toDouble
    val minValue = p.getMin.toDouble
    val scaler = new org.apache.spark.ml.feature.MinMaxScaler()
      .setInputCol("vector")
      .setOutputCol("scaledVector")
      .setMax(maxValue)
      .setMin(minValue)
    val df3 = scaler.fit(df2).transform(df2).drop("vector")
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.functions.udf
    val getScaledValue = udf( (v:Vector, i:Int) => v(i))
    var temp = df3
    for (i <- 0 until columnNames.length) {
      temp = temp.withColumn(
        columnNames(i),
        getScaledValue(df3("scaledVector"), lit(i))
      )
    }
    if(p.getWithMinMaxRange == true){
      result = temp.drop("scaledVector").coalesce(1).limit(dfColLength).toDF
    }else{
      result = temp.drop("scaledVector").toDF
    }
    val selectedDf = Utils.printNColumns(result,10)
    logger.debug("Output dataframe :" + selectedDf.show.toString)
    result
  }

  /**
   * Operates minMaxScaling function using following params.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = minMaxScaling(df)
}

object MinMaxScalingOperator {
  def apply(o: StreamOperatorInfo): MinMaxScalingOperator =
    new MinMaxScalingOperator(o)
}