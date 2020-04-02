package ksb.csle.component.operator.cleaning

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Stack
import scala.util.control.Breaks._

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.{GeneralizedLinearRegression => MlGeneralizedLinearRegression}
import org.apache.spark.ml.linalg.Vectors

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.GeneralizedLinearRegressionInfo
import ksb.csle.common.proto.StreamOperatorProto.GeneralizedLinearRegressionInfo._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs Generalized Linear Regression.
 * It generalizes linear regression by associating linear model with response
 * variables that have error distribution models through link function and the
 * magnitude of variance.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.GeneralizedLinearRegressionInfo]]
 *          GeneralizedLinearRegressionInfo contains attributes as follows:
 *          - familyType: Type of family. Enum(GAUSSIAN, BINOMIAL, POISSON,
 *                        GAMMA) (required)
 *                        GAUSSIAN: Data should be numeric (real or integer).
 *                        BINOMIAL: Data should be binominal or polynominal
 *                                  with 2 classes.
 *                        POISSON: Data should be numeric and non-negative
 *                                 (integer).
 *                        GAMMA: Data should be numeric and continuous and
 *                               positive(real or integer).
 *          - linkType: Type of link function which relates the linear predictor
 *                      to the distribution function. Enum(IDENTITY, LOG,
 *                      INVERSE, LOGIT, PROBIT, CLOGLOG, SQRT) (required)
 *          - maxIter: Maximum number of iterations. (required)
 *
 *  ==GeneralizedLinearRegressionInfo==
 *  {{{
 *  message GeneralizedLinearRegressionInfo {
 *  required string labelName = 3
 *  required FamilyType familyType = 4 [default = GAUSSIAN];
 *  required LinkType linkType = 5 [default = IDENTITY];
 *  required int32 maxIter = 6;
 *  enum FamilyType {
 *      GAUSSIAN = 0;
 *      BINOMIAL = 1;
 *      POISSON = 2;
 *      GAMMA = 3;
 *  }
 * enum LinkType {
 *      IDENTITY = 0;
 *      LOG = 1;
 *      INVERSE = 2;
 *      LOGIT = 3;
 *      PROBIT = 4;
 *      CLOGLOG = 5;
 *      SQRT = 6;
 *  }
 *  }
 *  }}}
 */
class GeneralizedLinearRegressionOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) with BaseDistanceCalculator {

  val p: ksb.csle.common.proto.StreamOperatorProto.GeneralizedLinearRegressionInfo =
    o.getGeneralizedLinearRegression

  /**
   * Validates GeneralizedLinearRegression info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame) {
    try {
      val familyName: String = getFamilyName(p.getFamilyType)
      if(familyName.equals("")) {
        throw new DataException(
            s"FailyType is invalidate : $p.getFamilyType")
      }
      val linkName: String = getLinkName(p.getLinkType)
      if(linkName.equals("")) {
        throw new DataException(
            s"LinkType is invalidate : $p.getLinkType")
      }
      val (check, description) = checkType(p.getFamilyType, p.getLinkType)
      if(!check) {
        throw new DataException(
            s"FamilyType and LinkType are not matched : $description")
      }
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
      if(!df.columns.contains(p.getLabelName))
        throw new DataException("Label column is not exist")
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
  }

  /**
   * Runs GeneralizedLinearRegression operation using following params.
   */
  @throws(classOf[KsbException])
  private def generalizedLinearRegression(df: DataFrame): DataFrame = {
    validate(df)
    try {
      var label_df: DataFrame = df.select(p.getLabelName)
      val vectors = df.drop(p.getLabelName).rdd.map(row => {
        Vectors.dense(row.toSeq.toArray.map({
          case l: Long => l
          case i: Int => i
          case d: Double => d
          case f: Float => f
        }))
      })
      var features_df: DataFrame =
        df.sparkSession.createDataFrame(
            vectors.map(Tuple1.apply)).toDF("features")
      val w = Window.partitionBy(lit(1)).orderBy(lit(1))
      label_df = label_df.withColumn("INDEX", row_number.over(w))
      features_df = features_df.withColumn("INDEX", row_number.over(w))
      label_df =
        label_df.join(features_df, "INDEX").sort(asc("INDEX")).drop("INDEX")
      val glr = new MlGeneralizedLinearRegression()
        .setFeaturesCol("features")
        .setFamily(getFamilyName(p.getFamilyType))
        .setLink(getLinkName(p.getLinkType))
        .setMaxIter(p.getMaxIter)
        .setLabelCol(p.getLabelName)
      val model = glr.fit(label_df)
      var result = df.sparkSession.createDataFrame(
          df.drop(p.getLabelName).columns.map(Tuple1.apply))
        .toDF("attribute")
        .withColumn("INDEX", row_number.over(w))
      var weight_result: DataFrame = result.sparkSession.createDataFrame(
          model.coefficients.toArray.map(Tuple1.apply))
        .toDF("weight")
        .withColumn("INDEX", row_number.over(w))
      result =
        result.join(weight_result, "INDEX").sort(asc("INDEX")).drop("INDEX")
      println(s"Coefficients: ${model.coefficients}")
      println(s"Intercept: ${model.intercept}")
      result
    } catch {
      case e: Exception => throw new ProcessException(
          s"GeneralizedLinearModel Process Error : ", e)
    }
  }

  private def getFamilyName(familyType: FamilyType): String = {
    var familyName: String = ""
    if(familyType == FamilyType.GAUSSIAN)
      familyName = "gaussian"
    else if(familyType == FamilyType.BINOMIAL)
      familyName = "binomial"
    else if(familyType == FamilyType.POISSON)
      familyName = "poisson"
    else if(familyType == FamilyType.GAMMA)
      familyName = "gamma"
    familyName
  }

  private def getLinkName(linkType: LinkType): String = {
    var linkName: String = ""
    if(linkType == LinkType.IDENTITY)
      linkName = "identity"
    else if(linkType == LinkType.LOG)
      linkName = "log"
    else if(linkType == LinkType.INVERSE)
      linkName = "inverse"
    else if(linkType == LinkType.LOGIT)
      linkName = "logit"
    else if(linkType == LinkType.PROBIT)
      linkName = "probit"
    else if(linkType == LinkType.CLOGLOG)
      linkName = "cloglog"
    else if(linkType == LinkType.SQRT)
      linkName = "sqrt"
    linkName
  }

  private def checkType(
      familyType: FamilyType, linkType: LinkType): (Boolean, String) = {
    var check: Boolean = true
    var descrition: String = ""
    if(familyType == FamilyType.GAUSSIAN) {
      if(!(linkType == LinkType.IDENTITY
          || linkType == LinkType.LOG
          || linkType == LinkType.INVERSE)) {
        descrition = "Gaussian : identity, log, inverse"
        check = false
      }
    }
    else if(familyType == FamilyType.BINOMIAL) {
      if(!(linkType == LinkType.LOGIT ||
          linkType == LinkType.PROBIT ||
          linkType == LinkType.CLOGLOG)) {
        descrition = "Binomial : logit, probit, cloglog"
        check = false
      }
    }
    else if(familyType == FamilyType.POISSON) {
      if(!(linkType == LinkType.LOG ||
          linkType == LinkType.IDENTITY ||
          linkType == LinkType.SQRT)) {
        descrition = "Poisson : log, identity, sqrt"
        check = false
      }
    }
    else if(familyType == FamilyType.GAMMA) {
      if(!(linkType == LinkType.INVERSE ||
          linkType == LinkType.IDENTITY ||
          linkType == LinkType.LOG)) {
        descrition = "Gamma : inverse, identity, log"
        check = false
      }
    }
    (check, descrition)
  }

  /**
   * Operates GeneralizedLinearRegression.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = generalizedLinearRegression(df)
}

object GeneralizedLinearRegressionOperator {
  def apply(o: StreamOperatorInfo): GeneralizedLinearRegressionOperator =
    new GeneralizedLinearRegressionOperator(o)
}