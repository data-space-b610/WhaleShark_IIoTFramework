package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.expressions.Window

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.PrincipalComponentAnalysisInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.operator.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs Principal Component Analysis. It converts a set of
 * observations of possibly correlated variables into a set of values of
 * linearly uncorrelated variables called principal components.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.PrincipalComponentAnalysisInfo]]
 *          PrincipalComponentAnalysisInfo contains attributes as follows:
 *          - k_value: Number of components (required)
 *
 *  ==PrincipalComponentAnalysisInfo==
 *  {{{
 *  message PrincipalComponentAnalysisInfo {
 *  required int32 k_value = 3 [default = 2];
 *  }
 *  }}}
 */
class PCAOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.PrincipalComponentAnalysisInfo =
    o.getPrincipalComponentAnalysis

  /**
   * validates PrincipalComponentAnalysis info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
    try {
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
      if(p.getKValue > df.columns.size || p.getKValue <= 0)
        throw new DataException("K value is more than columns size or " +
          "k value is Less than or equal to zero")
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
    df
  }

  /**
   * Run PrincipalComponentAnalysis operation using following params.
   */
  @throws(classOf[KsbException])
  private def principalComponentAnalysis(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : PrincipalComponentAnalysis")
    val k: Int = p.getKValue
    var result: DataFrame = null
    validate(df)
    val averageList = CalculatorUtils.getAverageListAllFromDF(df)
    try {
      val adjustDataVectors = df.rdd.map(row => {
        var averageListIndex = 0
        Vectors.dense(row.toSeq.toArray.map({
          case l: Long => {
            averageListIndex += 1
            l - averageList(averageListIndex-1)
          }
          case i: Int => {
            averageListIndex += 1
            i - averageList(averageListIndex-1)
          }
          case d: Double => {
            averageListIndex += 1
            d - averageList(averageListIndex-1)
          }
          case f: Float => {
            averageListIndex += 1
            f - averageList(averageListIndex-1)
          }
        }))
      })
      val adjustDf =
        df.sparkSession.createDataFrame(adjustDataVectors.map(Tuple1.apply))
         .toDF("features")
      adjustDf.show()
      val pca = new PCA()
        .setInputCol("features")
        .setOutputCol("result")
        .setK(k)
        .fit(adjustDf)
      val pca_df = pca.transform(adjustDf)
      println(pca.explainedVariance.toArray.toList) //proportion of variance
      println(pca.pc.values.toList) //vectors
      result = pca_df.select("result")
      val vectorToColumn = udf{ (x: DenseVector, index: Int) => x(index) }
      for(i <- 0 until k) {
        var columnName: String = "pc" + (i + 1)
        result =
          result.withColumn(
              columnName, vectorToColumn(col("result"),lit(i)))
      }
      result = result.drop("result")
    } catch {
      case e: Exception => throw new ProcessException(
          s"PrincipalComponentAnalysis Process Error : ", e)
    }
    result
  }

  /**
   * Operates PrincipalComponentAnalysis.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = principalComponentAnalysis(df)
}

object PCAOperator {
  def apply(o: StreamOperatorInfo): PCAOperator =
    new PCAOperator(o)
}