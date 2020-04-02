package ksb.csle.component.operator.transformation

import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto.InterpolationMethodOption
import ksb.csle.component.operator.util._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that converts the dataframe at regular intervals by interpolation.
 * LINEAR: curve fitting using linear polynomials to construct new data points.
 * NEAREST: Nearest-neighbor interpolation, selects the value of the nearest point.
 * PREV_COPY: selects the previous value.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.InterpolationInfo]]
 *          InterpolationInfo contains attributes as follows:
 *          - indexColumnName: Column name of index column (required)
 *          - stepSize: Intervals of index range (required)
 *          - methodOption: Interpolation method. Enum(LINEAR, NEAREST, PREV_COPY)
 *                          (required)
 *          - keepColumnType: Parameter to define whether to keep the original
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
 */
class InterpolateOperator(
    o: StreamOperatorInfo
    ) extends BaseGenericOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.InterpolationInfo =
    o.getInterpolation

  private def calY_str(
      x: Int, df: DataFrame, keyName: String, strCol: String): String = {
    val result: String = p.getMethodOption match {
      case InterpolationMethodOption.NEAREST => calY_nearest_str(x,df,keyName,strCol)
      case _ => calY_prev_str(x,df,keyName,strCol)
    }
    result
  }
  private def calY(
      x: Int, df: DataFrame, keyName: String, strCol: String): Double = {
    val result: Double = p.getMethodOption match {
      case InterpolationMethodOption.NEAREST => calY_nearest(x,df,keyName,strCol)
      case InterpolationMethodOption.LINEAR => calY_linear(x,df,keyName,strCol)
      case _ => calY_prev(x,df,keyName,strCol)
    }
    result
  }

  private def calY_linear(
      x: Int, df: DataFrame, keyName: String, strCol: String): Double = {
    val diff = col(keyName) - x
    val diff_abs =  abs(diff)

    val df_x0_select = df.select(expr(keyName), expr(strCol).cast("double"),
        diff.alias("diff"), diff_abs.alias("diff_abs"))
        .sort("diff_abs")
        .where("diff <= 0")
        .limit(1).collect()
    val df_x1_select = df.select(expr(keyName), expr(strCol).cast("double"),
        diff.alias("diff"), diff_abs.alias("diff_abs"))
        .sort("diff_abs")
        .where("diff >= 0")
        .limit(1).collect()

    val x0 = df_x0_select(0).getInt(0).toDouble
    val y0 = df_x0_select(0).getDouble(1)
    val x1 = df_x1_select(0).getInt(0).toDouble
    val y1 = df_x1_select(0).getDouble(1)

    val cond = x1-x0
    val y = cond match {
      case 0 => y0
      case _ => y0 + (x-x0)*((y1-y0)/(x1-x0))
    }
    y
  }

  private def calY_nearest_str(
      x: Int, df: DataFrame, keyName: String, strCol: String): String = {
    val diff = col(keyName) - x
    val diff_abs =  abs(diff)

    val df_select = df.select(expr(strCol).cast("string"), diff_abs.alias("diff_abs"))
        .sort("diff_abs")
        .limit(1).collect()

    val y = df_select(0).getString(0)
    y
  }
  private def calY_nearest(
      x: Int, df: DataFrame, keyName: String, strCol: String): Double = {
    val diff = col(keyName) - x
    val diff_abs =  abs(diff)

    val df_select = df.select(
        expr(strCol).cast("double"), diff_abs.alias("diff_abs"))
        .sort("diff_abs")
        .limit(1).collect()

    val y = df_select(0).getDouble(0)
    y
  }

  private def calY_prev_str(
      x: Int, df: DataFrame, keyName: String, strCol: String): String = {
    val diff = col(keyName) - x
    val diff_abs =  abs(diff)

    val df_x0_select = df.select(expr(strCol).cast("string"),
        diff.alias("diff"), diff_abs.alias("diff_abs"))
        .sort("diff_abs")
        .where("diff <= 0")
        .limit(1).collect()

    val y = df_x0_select(0).getString(0)
    y
  }
  private def calY_prev(
      x: Int, df: DataFrame, keyName: String, strCol: String): Double = {
    val diff = col(keyName) - x
    val diff_abs =  abs(diff)

    val df_x0_select = df.select(expr(strCol).cast("double"),
        diff.alias("diff"), diff_abs.alias("diff_abs"))
        .sort("diff_abs")
        .where("diff <= 0")
        .limit(1).collect()

    val y = df_x0_select(0).getDouble(0)
    y
  }

  private def type_define(
      src: DataFrame, dst: DataFrame, strCol: Array[String]): DataFrame = {
    var df = dst
      for (i <- 0 until strCol.length) {
        val tp = src.schema.apply(strCol(i)).dataType
        df = df.withColumn(strCol(i)+"_new", df.col(strCol(i)).cast(tp))
          .drop(strCol(i))
          .withColumnRenamed(strCol(i)+"_new", strCol(i))
      }
    df
  }

  private def makeDataFrame(sp: SparkSession, arrayx: Range,
      df: DataFrame, keyName: String, strCol: String): DataFrame = {
    val isNumeric = DataTypeUtil.isNumericType(df.schema.apply(strCol).dataType)
    val newdf = isNumeric match {
      case true => {
        val arrayy = arrayx.map {x => (x, calY(x,df,keyName,strCol) ) }
        sp.sqlContext.createDataFrame(arrayy).toDF(keyName, strCol)
      }
      case false => {
         val arrayy_str = arrayx.map {x => (x, calY_str(x,df,keyName,strCol) ) }
         sp.sqlContext.createDataFrame(arrayy_str).toDF(keyName, strCol)
      }
    }
    newdf
  }

  private def interpolation(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : Interpolation")

    val keyName = p.getIndexColumnName.isEmpty() match {
      case true => Utils.getSingleColumnName(0, df)
      case false => p.getIndexColumnName
    }
    val strCol = df.columns.filter(! _.contentEquals(keyName))
    val nCol = strCol.length
    val step = p.getStepSize
    val x_start = df.select(min(keyName)).take(1).apply(0).getInt(0)
    val x_end = df.select(max(keyName)).take(1).apply(0).getInt(0)
    logger.info("x_start: " +x_start + " x_end: " + x_end)

      val sp = df.sparkSession
    import sp.implicits._
    import sp.sqlContext.implicits._
    val sc = sp.sparkContext

    val arrayx = (x_start to x_end by step)
    var newdf = makeDataFrame(sp, arrayx, df, keyName, strCol(0))

    for (i <- 1 until nCol) {
      val tmpdf = makeDataFrame(sp, arrayx, df, keyName, strCol(i))
      newdf = newdf.join(tmpdf, Seq(keyName))
    }

    if( p.getKeepColumnType ) {
      newdf = type_define(df, newdf, strCol)
    }

    newdf.show()
    newdf
  }

  override def operate(df: DataFrame): DataFrame = interpolation(df)

  override def stop: Unit = ()
}