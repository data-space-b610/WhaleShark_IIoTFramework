package ksb.csle.didentification

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import ksb.csle.common.base.result.DefaultResult

trait DataSuite extends BeforeAndAfterAll {
  this: Suite =>
  var _spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkConf: SparkConf = new SparkConf()
    _spark = SparkSession.builder()
       .config(sparkConf)
       .master("local[*]")
       .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
  }

  def spark: SparkSession = _spark

  def sc : SparkContext = _spark.sparkContext

  def result: DefaultResult = DefaultResult("s", "p", "o")

}
