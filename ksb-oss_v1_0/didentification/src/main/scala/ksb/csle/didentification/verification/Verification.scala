package ksb.csle.didentification.verification

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.DataFrame

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto._

import ksb.csle.didentification.interfaces.{InformationLossBound, DataFrameCheck}
import ksb.csle.didentification.verification.loss._
import ksb.csle.didentification.verification.risk._
import ksb.csle.didentification.verification.check._

class Verification(p: PrivacyCheckInfo) extends DataFrameCheck {
  
  val privacy: PrivacyCheckInfo = p
  
  def printAnonymizeResult(
      src: DataFrame, 
      result: DataFrame,
      fieldInfos: Array[FieldInfo]): Unit = {
//    val fieldInfos = (privacy.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    val quasiColumns = getColumnNames(src, getQuasiColumnIDs(fieldInfos))
    val sensColumns = getColumnNames(src, getSensColumnIDs(fieldInfos))
    val loss = measureLoss(src, result, quasiColumns, privacy.getMeasureLoss)
    printLossInfo(loss, privacy.getMeasureLoss)
    
    val (srcRisk, anonymizedRisk) =
      measureRisk(src, result, quasiColumns, privacy.getMeasureRisk)
    printRiskInfo(srcRisk, anonymizedRisk, privacy.getMeasureRisk)
    
    val (srcAnonymity, anonymizedAnonymity) = measureAnonymity(
      src, result, quasiColumns, sensColumns, privacy.getCheckAnonymity)
    printAnonymityInfo(srcAnonymity, anonymizedAnonymity, privacy.getCheckAnonymity)
  }

  def measureLoss(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      columnNames: Array[String],
      lossType: LossMeasureMethod): InformationLossBound =
    measureLoss(src, anonymizedSrc, anonymizedSrc, columnNames, lossType)

  def measureLoss(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      suppressedSrc: DataFrame,
      columnNames: Array[String],
      lossType: LossMeasureMethod): InformationLossBound = {
    lossType match {
      case LossMeasureMethod.CARDINALITY =>
        CardinalityLoss().lossMeasure(src, anonymizedSrc, suppressedSrc, columnNames)
      case LossMeasureMethod.EQUIVALENCE =>
        EquivalenceLoss().lossMeasure(src, anonymizedSrc, suppressedSrc, columnNames)
//      case LossMeasureMethod.TREE =>
//        HierarchicalTreeLoss(hierarchies)
      case LossMeasureMethod.AECS =>
        AECSLoss().lossMeasure(src, anonymizedSrc, suppressedSrc, columnNames)
      case LossMeasureMethod.DM =>
        DMLoss().lossMeasure(src, anonymizedSrc, suppressedSrc, columnNames)
      case _ =>
        AECSLoss().lossMeasure(src, anonymizedSrc, suppressedSrc, columnNames)
    }
  }

  def printLossInfo(
      loss: InformationLossBound,
      lossType: LossMeasureMethod): Unit = {
    println("#################################################")
    val msg = lossType match {
      case LossMeasureMethod.CARDINALITY => "CARDINALITY"
      case LossMeasureMethod.EQUIVALENCE => "EQUIVALENCE"
      case LossMeasureMethod.TREE => "HIERARHCHICAL TREE"
      case LossMeasureMethod.AECS => "AECS"
      case LossMeasureMethod.DM =>  "DM"
      case _ => "ACES"
    }
    printLossInfo(msg, loss)
    println("#################################################")
  }

  def printLossInfo(info: String, loss: InformationLossBound) = {
    println("#################################################")
    println(info + " LOSS: " + loss.loss +
        "\tRelative LOSS: " + loss.relativeLoss)
    println(info + " LOSS (Lower Bound): " + loss.lowerBound +
        "\tRelative LOSS (Lower Bound): " + loss.relativelowerBound)
    println("#################################################")
  }

  def measureRisk(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      columnNames: Array[String],
      riskType: RiskMeasureMethod): (Double, Double) = {
    val srcRisk = measureRisk(src, columnNames, riskType)
    val anonymizedRisk = measureRisk(anonymizedSrc, columnNames, riskType)
    (srcRisk, anonymizedRisk)
  }

  private def measureRisk(
      src: DataFrame,
      columnNames: Array[String],
      riskType: RiskMeasureMethod): Double = {
    riskType match {
      case RiskMeasureMethod.UNIQUENESS =>
        ReIdentificationRisk().riskMeasure(src, columnNames)
      case RiskMeasureMethod.GLOBALRISK =>
        GlobalRiskMeasure().riskMeasure(src, columnNames)
      case _ => GlobalRiskMeasure()
        ReIdentificationRisk().riskMeasure(src, columnNames)
    }
  }

  def printRiskInfo(
      srcRisk: Double,
      anonymizedRisk: Double,
      riskType: RiskMeasureMethod): Unit = {
    println("#################################################")
    val msg = riskType match {
      case RiskMeasureMethod.UNIQUENESS => "Uniqueness"
      case RiskMeasureMethod.GLOBALRISK => "Global Risk"
      case _ => "Unisquess"
    }
    printRiskInfo(msg, srcRisk, anonymizedRisk)
    println("#################################################")
  }

  private def printRiskInfo(
      info: String,
      srcRisk: Double,
      anonymizedRisk: Double):Unit = {
    println(info + " REIDENTIFICATION RISK (Original): " + srcRisk)
    println(info + " REIDENTIFICATION RISK (Anonymized): " + anonymizedRisk)
  }

  def measureAnonymity(
      src: DataFrame,
      anonymized: DataFrame,
      columnNames: Array[String],
      sens: Array[String],
      checkType: CheckAnonymityMethod): (Double, Double) = {
    val srcAnonymity = measureAnonymity(src, columnNames, sens, checkType)
    val anonymizedAnonymity = measureAnonymity(anonymized, columnNames, sens, checkType)

    (srcAnonymity, anonymizedAnonymity)
  }
  
  def measureAnonymity(
      src: DataFrame,
      columnNames: Array[String],
      sens: Array[String],
      checkType: CheckAnonymityMethod): Double = {
    checkType match {
      case CheckAnonymityMethod.KANONYMITYCHECK =>
        KAnonymityCheck().getKAnonymityValue(src, columnNames)
      case CheckAnonymityMethod.LDIVERSITYCHECK =>
        CommonLDiversityCheck().getLDiversityValue(src, columnNames, sens)
      case CheckAnonymityMethod.PROLDIVERSITYCHECK =>
        ProbLDiversityCheck().getLDiversityValue(src, columnNames, sens)
      case CheckAnonymityMethod.ENTLDIVERSITYCHECK =>
        EntropyLDiversityCheck().getLDiversityValue(src, columnNames, sens)
      case _ =>
        KAnonymityCheck().getKAnonymityValue(src, columnNames)
    }
  }
  
  def printAnonymityInfo(
      sAnonymity: Double,
      aAnonymity: Double,
      checkType: CheckAnonymityMethod): Unit = {
    println("#################################################")
    val msg = checkType match {
      case CheckAnonymityMethod.KANONYMITYCHECK => "k-Anonymity"
      case CheckAnonymityMethod.LDIVERSITYCHECK => "l-Diversity"
      case CheckAnonymityMethod.PROLDIVERSITYCHECK => "Probablic l-Diversity"
      case CheckAnonymityMethod.ENTLDIVERSITYCHECK => "Entropy l-Diversity"
      case CheckAnonymityMethod.CLDIVERSITYCHECK => "k-Anonymity"
      case _ => "k-Anonymity"
    }
    printAnonymityInfo(msg, sAnonymity, aAnonymity)
    println("#################################################")
  }

  private def printAnonymityInfo(
      info: String,
      sAnonymity: Double,
      aAnonymity: Double): Unit = {
    println(info + " Value (Original): " + sAnonymity)
    println(info + " Value (Anonymized): " + aAnonymity)
  }

}

object Verification {
  def apply(p: PrivacyCheckInfo): Verification = new Verification(p)
}
