package ksb.csle.component.operator.util

import ksb.csle.component.exception._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object DataTypeUtil {

  def castAnyToDobule(in : Any): Double = {
    in match {
      case n : Number => in.asInstanceOf[Number].doubleValue
      case str : String => in.toString.toDouble
      case unexptected => throw new KsbException()
    }
  }

  def isNumericType(in: DataType): Boolean = {
    in match {
      case IntegerType => true
      case FloatType => true
      case DoubleType => true
      case LongType => true
      case _ => false
    }
  }
}
