package ksb.csle.component.operator.reduction

import com.google.protobuf.Message
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame,SQLContext, Row}
import org.apache.logging.log4j.scala.Logging
import org.apache.log4j.Level
import org.apache.log4j.Logger
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import org.apache.log4j.Level
import org.apache.log4j.Logger
import ksb.csle.common.proto.SharedProto._

/**
 * Test Class for Filter Using SQL function in data reduction package.
 */
case class Customer(id: Int, name: String, city: String, state: String, zipcode: String)

class FilterUsingSqlTest extends UnitSpec with DataFunSuite with Logging {

  @transient var customers: Seq[Customer] = _
  @transient var customersDF: DataFrame = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  override def beforeAll(): Unit = {
    super.beforeAll()
    customers = Seq(
      Customer(100, "John Smith", "Austin", "TX", "78727"),
      Customer(200, "Joe Johnson", "Dallas", "TX", "75201"),
      Customer(300, "Bob Jone", "Houston", "TX", "77028"),
      Customer(400, "Andy Davis", "San Antonio", "TX", "78227"),
      Customer(500, "James William", "Austin", "TX", "78727"))
    customersDF = convertDF()

  }

  def convertDF() : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val customersRDD = sc.parallelize(customers, 4)
    val df = customersRDD.toDF.repartition(1)
    df
  }

  "A FilterUsingSql" should "do filtering using SQL with condition" in {
    val SQL_WHERE: String = " <'100000' ORDER BY "
    val SQL_SELECT: String = "name,zipcode"
    val filterUsingSqlInfo = FilterUsingSqlInfo.newBuilder()
      .setSelectedColumnId(0)
      .addSubParam(
        SubParameter.newBuilder
          .setKey("sql_where")
          .setValue(SQL_WHERE)
          .build
        )
      .addSubParam(
        SubParameter.newBuilder
          .setKey("sql_select")
          .setValue(SQL_SELECT)
          .build
        )
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.FilterUsingSql")
      .setFilterUsingSql(filterUsingSqlInfo)
      .build
    val filterUsingSql = FilterUsingSqlOperator(operator1)
    val resultDF = filterUsingSql.operate(customersDF)
    val row = resultDF.take(1)
    val resultValue = row.map{ case Row(name: String, zip: String) => name}
    assert(resultValue.mkString === "John Smith")
  }
}
