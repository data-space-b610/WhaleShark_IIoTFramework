package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.ChiSquareSelectorInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs ChiSquared feature selector. It uses the Chi-Squared
 * test of independence to decide which features to choose ChiSquareSelector
 * value in a given dataframe
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.ChiSquareSelectorInfo]]
 *          ChiSquareSelectorInfo contains attributes as follows:
 *          - labelName: label Name (repeated)
 *          - numTopFeatures: Parameter to choose a fixed number of top features
 *                            according to a chi-squared test (required)
 *
 *  ==ChiSquareSelectorInfo==
 *  {{{
 *  message ChiSquareSelectorInfo {
 *  required string labelName = 1;
 *  required int32 numTopFeatures = 2 [default = 1];
 *  }
 *  }}}
 */
class ChiSquareSelectOperator(
    o: StreamOperatorInfo) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.ChiSquareSelectorInfo =
    o.getChiSquareSelector

  /**
   * validates ChiSquareSelector info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
    try {
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
      if(!df.columns.contains(p.getLabelName))
        throw new DataException("Label column is not exist")
      if(p.getNumTopFeatures < 1)
        throw new DataException("numTopFeatures is less than 1")
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
    df
  }

  /**
   * Run chiSquareSelector operation using following params.
   */
  @throws(classOf[KsbException])
  private def chiSquareSelector(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : chiSquareSelector")
    var result: DataFrame = null
    validate(df)

    try {
      val columns: Array[String] = getFeaturesColumns(df)
      val assembler = new VectorAssembler()
        .setInputCols(columns)
        .setOutputCol("features")
      val df2 = assembler.transform(df)
      val selector = new ChiSqSelector()
        .setNumTopFeatures(p.getNumTopFeatures)
        .setFeaturesCol("features")
        .setLabelCol(p.getLabelName)
        .setOutputCol("selectedFeatures")
      val chi_result = selector.fit(df2)
      var selectedColumnNameList: Array[String] = new Array[String](0)
      for (selectedColumnIndex <- chi_result.selectedFeatures.toList) {
          selectedColumnNameList :+= columns(selectedColumnIndex)
      }
      result = df.select(selectedColumnNameList.map(col):_*)
    } catch {
      case e: Exception => throw new ProcessException(
          s"ChiSquareSelector Process Error : ", e)
    }
    result
  }

  def getFeaturesColumns(df: DataFrame): Array[String] = {
    var result: Array[String] = new Array[String](0)
    var index: Int = 0
    for (field <- df.schema.fields) {
      if(field.dataType.isInstanceOf[NumericType]) {
        if(!field.name.equals(p.getLabelName)) {
          result :+= field.name
        }
      }
    }
    result
  }

  /**
   * Operates ChiSquareSelector.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = chiSquareSelector(df)
}

object ChiSquareSelectOperator {
  def apply(o: StreamOperatorInfo): ChiSquareSelectOperator =
    new ChiSquareSelectOperator(o)
}