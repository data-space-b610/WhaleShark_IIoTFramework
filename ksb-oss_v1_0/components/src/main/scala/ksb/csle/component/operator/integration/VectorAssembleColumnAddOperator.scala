package ksb.csle.component.operator.integration

import scala.collection.JavaConversions._

import com.google.protobuf.Message
import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.VectorUDT

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that adds vector assemble column into the input dataframe using a
 * given condition. The vector assemble column is used by ML predict.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AddVectorAssembleColumnInfo]]
 *          AddVectorAssembleColumnInfo contains attributes as follows:
 *          - vectorAssembleColumnName: name of vectorAssembleColumn (required)
 *
 *  ==AddVectorAssembleColumnInfo==
 *  {{{
 *  message AddVectorAssembleColumnInfo {
 *  required string vectorAssembleColumnName = 3;
 *  }
 *  }}}
 */
class VectorAssembleColumnAddOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.AddVectorAssembleColumnInfo =
    o.getAddVectorAssembleColumn

  private def addVectorAssembleColumn(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : AddVectorAssembleColumn")
    val schema = new StructType().add("features", new VectorUDT())
    val schemaFieldNames = df.schema.fieldNames.toSeq
    var schemaFieldType =  df.schema(0).dataType
    logger.debug("Data Type: " + schemaFieldType)
    val df2 = df.select(schemaFieldNames.map(c => df.col(c)
        .cast(schemaFieldType.typeName)): _*)
    val assembler = new VectorAssembler()
       .setInputCols(schemaFieldNames.toArray)
       .setOutputCol(p.getVectorAssembleColumnName)
    val result = assembler.transform(df2)
    logger.debug(df.show.toString)
    logger.debug(df.printSchema.toString)
    result
  }

  /**
   * Operates AddTimeIndexColumn.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = addVectorAssembleColumn(df)
}

object VectorAssembleColumnAddOperator {
  def apply(o: StreamOperatorInfo): VectorAssembleColumnAddOperator =
    new VectorAssembleColumnAddOperator(o)
}