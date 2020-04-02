package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.AggregateInfo
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry.FunctionType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs aggregation operation in the given dataframe.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AggregateInfo]]
 *          AggregateInfo contains attributes as follows:
 *          - attributes: Parameter to select the attributes and the aggregation
 *                        function to apply on them (repeated)
 *          - groupByAttributeName: Attribute names to be applied by groupby
 *                                  operation. (repeated)
 *
 *  ==AggregateInfo==
 *  {{{
 *  message AggregateInfo {
 *  repeated AttributeEntry attributes = 3;
 *  repeated string groupByAttributeName = 4;
 *  }
 *  }}}
 */
class AggregateOperator(
    o: StreamOperatorInfo) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.AggregateInfo =
    o.getAggregate

  /**
   * validates Aggregate info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame) {
    try {
      if (p.getAttributesList.size <= 0) {
        throw new DataException(
            "Size of Attribute Entry is Zero : " +
            s"$p.getAttributesList.size")
      }
      if (p.getGroupByAttributeNameList.size <= 0) {
        throw new DataException(
            "GroupByAttributeName is not defined : " +
            s"$p.getGroupByAttributeNameList.size")
      }
      for (i <- 0 until p.getAttributesList.size) {
        val attributeName: String =
          p.getAttributesList.get(i).getAttributeName
        val functionType: FunctionType =
          p.getAttributesList.get(i).getFunctionType
        if(!df.columns.contains(attributeName)) {
          throw new DataException(
              "AttributeName is not exist in DataFrame : " +
              s"$attributeName")
        }
        if(functionType == FunctionType.CONCATENATION) {
          for (field <- df.schema.fields) {
            if(field.name.equals(attributeName)) {
              if(!field.dataType.isInstanceOf[StringType]) {
                throw new DataException(
                    s"Attribute ($attributeName) Type is not StringType : " +
                    s"$field")
              }
            }
          }
        }
        else if(functionType == FunctionType.AVERAGE ||
            functionType == FunctionType.SUM) {
          for (field <- df.schema.fields) {
            if(field.name.equals(attributeName)) {
              if(!field.dataType.isInstanceOf[NumericType]) {
                throw new DataException(
                    s"Attribute ($attributeName) Type is not NumericType : " +
                    s"$field")
              }
            }
          }
        }
      }
      for (groupByAttributeName <- p.getGroupByAttributeNameList.toArray) {
        if(!df.columns.contains(groupByAttributeName)) {
          throw new DataException(
              "GroupByAttributeName is not exist in DataFrame : " +
              s"$groupByAttributeName")
        }
      }
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
  }

  /**
   * Run sampleStratified operation using following params.
   */
  @throws(classOf[KsbException])
  private def aggregate(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : Aggregate")
    logger.info(df.show.toString)
    validate(df)
    var result: DataFrame = null
    try {
      val groupCols: Array[Column] =
        p.getGroupByAttributeNameList.toArray.map(x => df.col(x.toString()))
      val functions: Array[Column] =
        p.getAttributesList
          .map(x => function_mapping(x.getAttributeName, x.getFunctionType))
          .toArray
      result =
        df.groupBy(groupCols:_*)
          .agg(functions.head, functions.tail:_*)
          .sort(groupCols:_*)
    } catch {
      case e: Exception => throw new ProcessException(
          s"Aggregate Process Error : ", e)
    }
    result
  }

  def function_mapping(
      attributeName: String, functionType: FunctionType): Column = {
    functionType match {
      case FunctionType.AVERAGE => functions.avg(
          attributeName).as(attributeName + "_avg")
      case FunctionType.SUM => functions.sum(
          attributeName).as(attributeName + "_sum")
      case FunctionType.COUNT => functions.count(
          attributeName).as(attributeName + "_count")
      case FunctionType.CONCATENATION => functions.concat_ws(
          "|", collect_list(attributeName)).as(attributeName + "_concat")
    }
  }

  /**
   * Operates Aggregate.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = aggregate(df)
}

object AggregateOperator {
  def apply(o: StreamOperatorInfo): AggregateOperator =
    new AggregateOperator(o)
}