package ksb.csle.component.operator.transformation

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto.InterpolationInfo
import ksb.csle.component.ingestion.util._


/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that joins two dataframes according to timestamp.
 * The dataframe to be joined (second dataframe) is loaded by csv file format.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.TimeSynchronizationInfo]]
 *          TimeSynchronizationInfo contains attributes as follows:
 *          - hasTimeDelay: Parameter to define whether second dataframe has
 *                          a time delay (required)
 *          - isAutoMode: If this parameter is true, time delay is calculated
 *                        automatically (optional)
 *                        (as smallest difference between two dataframes).
 *                        If this parameter is false, next parameter
 *                        is used.
 *          - timeDelay: Paremeter to specify timeDelay if above parameter is
 *                       false (optional)
 *          - interpolationInfo: interpolation operator of first dataframe
 *                               (required)
 *          - isTimeStampColumn: Parameter to define whether selected column is
 *                               timestamp type (required)
 *                               If this parameter is false, next parameter is
 *                               used.
 *          - userTimeStampPattern: Parameter to specify the timestamp pattern
 *                                  of selected column if above parameter is
 *                                  false (optional)
 *          - joinDataInfo: Parameter to specify the parameters of second
 *                          dataframe for reading and interpolation. (required)
 *
 *          [[ksb.csle.common.proto.StreamOperatorProto.TimeSyncDataFrameInfo]]
 *          TimeSyncDataFrameInfo contains attributes as follows:
 *          - filePath: File path of second dataframe (required)
 *          - hasHeader: Parameter to define whether csv file has header (optional)
 *          - separator: separator of csv file (optional)
 *          - interpolationInfo: interpolation operator of second dataframe
 *                               (required)
 *          - isTimeStampColumn: Parameter to define whether selected column is
 *                               timestamp type (required)
 *                               If this parameter is false, next parameter is
 *                               used.
 *          - userTimeStampPattern: Parameter to specify the timestamp pattern
 *                                  of selected column if above parameter is
 *                                  false (optional)
 *
 *          [[ksb.csle.common.proto.StreamOperatorProto.InterpolationInfo]]
 *          InterpolationInfo contains attributes as follows:
 *          - indexColumnName: Column name of timestamp column (required)
 *          - stepSize: Intervals of time range, unit is second (optional)
 *          - methodOption: Interpolation method (required)
 *          - keepColumnType: Parameter to define whether to keep the originals
 *                            column type (optional)
 *                            After interpolation option, the column type is
 *                            changed to double type.
 *
 *  ==InterpolationInfo==
 *  {{{
 *  message InterpolationInfo {
 *  required string indexColumnName = 1;
 *  required int32 stepSize = 2 [default = 1];
 *  required InterpolationMethodOption methodOption = 3 [default = LINEAR];
 *  optional bool keepColumnType = 4 [default = false];
 *  }
 *  }}}
  *  ==TimeSyncDataFrameInfo==
 *  {{{
 *  message TimeSyncDataFrameInfo {
 *  required string filePath = 1;
 *  optional bool hasHeader = 2 [default = true];
 *  optional string separator = 3 [default = ","];
 *  required InterpolationInfo interpolationInfo = 4; 
 *  required bool isTimeStampColumn = 5 [default = true];
 *  optional string userTimeStampPattern = 6 [default = "yyyy-mm-dd hh:mm:ss"];
 *  }
 *  }}}
 *  ==TimeSynchronizationInfo==
 *  {{{
 *  message TimeSynchronizationInfo {
 *  required bool hasTimeDelay = 1 [default = false];
 *  optional bool isAutoMode = 2 [default = true];
 *  optional int32 timeDelay = 3;
 *  required InterpolationInfo interpolationInfo = 4; 
 *  required bool isTimeStampColumn = 5 [default = true];
 *  optional string userTimeStampPattern = 6 [default = "yyyy-mm-dd hh:mm:ss"];
 *  required TimeSyncDataFrameInfo joinDataInfo = 7;
 *  }
 *  }}}
 */
class TimeSynchronizeOperator(
    o: StreamOperatorInfo) extends BaseGenericOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.TimeSynchronizationInfo =
    o.getTimeSynchronization

  val interpolationOp_df1 = StreamOperatorInfo.newBuilder()
    .setId(100)
    .setPrevId(0)
    .setClsName("ksb.csle.component.operator.ransformation.Interpolation")
    .setInterpolation(p.getInterpolationInfo)
    .build

  val interpolationOp_df2 = StreamOperatorInfo.newBuilder()
    .setId(101)
    .setPrevId(100)
    .setClsName("ksb.csle.component.operator.ransformation.Interpolation")
    .setInterpolation(p.getJoinDataInfo.getInterpolationInfo)
    .build

  val interpolation_df1 = new InterpolateOperator(interpolationOp_df1)
  val interpolation_df2 = new InterpolateOperator(interpolationOp_df2)

  private def readData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df = spark.read
      .option("inferSchema", "true")
      .option("header", p.getJoinDataInfo.getHasHeader)
      .option("sep", p.getJoinDataInfo.getSeparator)
      .csv(p.getJoinDataInfo.getFilePath)
    df
  }

  private def calTimeDelay(df: DataFrame, colName: String,
      ref: DataFrame, refColName: String): Int ={
    val ref_select = ref.selectExpr(refColName).limit(1).collect()
    val refTime = ref_select(0).getInt(0)

    val diff = col(colName) - refTime
    val diff_abs = abs(diff)
    val df_select = df.select(diff.alias("diff"), diff_abs.alias("diff_abs"))
        .sort("diff_abs")
        .limit(1).collect()
    val delay = df_select(0).getInt(0)
    delay
  }

  private def calTimeOffset(df: DataFrame, colName: String,
      ref: DataFrame, refColName: String, autoMode: Boolean, delay: Int): DataFrame = {
    val offset = autoMode match {
      case true => calTimeDelay(df, colName, ref, refColName)
      case false => delay
    }
    logger.info("time offset: " + offset)

    val newdf = df.withColumn(colName+"_new_", df.col(colName) - offset )
        .drop(colName)
        .withColumnRenamed(colName+"_new_", colName)
    newdf
  }

  private def convertTimeStampColumn(df: DataFrame, colName: String,
      userTimeStampPattern: String): DataFrame = {
    val newdf = df.schema.apply(colName).dataType match {
      case TimestampType => df.withColumn(colName+"_new_", df.col(colName).cast("int"))
          .drop(colName)
          .withColumnRenamed(colName+"_new_", colName)
      case _ => df.withColumn(colName+"_new_",
          unix_timestamp(df.col(colName), userTimeStampPattern).cast("timestamp").cast("int") )
          .drop(colName)
          .withColumnRenamed(colName+"_new_", colName)
    }
    newdf
  }

  private def timeSyncronization(df1: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : TimeSyncronization")

    val spark = df1.sparkSession
    val df2 = readData(spark)
//    df2.printSchema()

    val colName1: String = p.getInterpolationInfo.getIndexColumnName
    val indexdf1 = p.getIsTimeStampColumn match {
      case true => convertTimeStampColumn(df1, colName1, p.getUserTimeStampPattern)
      case false => df1
    }

    val colName2: String = p.getJoinDataInfo.getInterpolationInfo.getIndexColumnName
    val tempdf2 = p.getJoinDataInfo.getIsTimeStampColumn match {
      case true => convertTimeStampColumn(df2, colName2, p.getJoinDataInfo.getUserTimeStampPattern)
      case false => df2
    }

    val indexdf2 = p.getHasTimeDelay match {
      case true => calTimeOffset(tempdf2, colName2, indexdf1, colName1,
          p.getIsAutoMode, p.getTimeDelay)
      case false => tempdf2
    }

    val outdf1 = interpolation_df1.operate(indexdf1)
    val outdf2 = interpolation_df2.operate(indexdf2)

    val joindf = colName1.contentEquals(colName2) match {
      case true => outdf1.join(outdf2, Seq(colName1))
      case false => {
        val outdf2_temp = outdf2.withColumnRenamed(colName2, colName1)
        outdf1.join(outdf2_temp, Seq(colName1))
      }
    }
    val newdf = p.getIsTimeStampColumn match {
      case true => joindf.withColumn(colName1+"_temp_",
          joindf.col(colName1).cast(TimestampType))
          .drop(colName1)
          .withColumnRenamed(colName1+"_temp_", colName1)
      case false => joindf
    }
    newdf.show()
    newdf
  }

  override def operate(df: DataFrame):
      DataFrame = timeSyncronization(df)

  override def stop: Unit = ()
}
