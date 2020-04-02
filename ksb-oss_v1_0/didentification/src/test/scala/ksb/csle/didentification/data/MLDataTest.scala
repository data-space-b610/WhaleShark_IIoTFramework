package ksb.csle.didentification

import org.apache.logging.log4j.scala.Logging
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo._
import ksb.csle.common.proto.StreamDidentProto._

import ksb.csle.didentification.utilities._

/**
 * Test class for making default data frame, in order to check
 * the functionalities of de-identification algorithms 
 */
class MLDataTest extends UnitSpec 
  with DataSuite with Logging {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  @transient var data: DataFrame = _
  var opId: Int = 0

  case class DefaultColumnInfo(id: Int, colType: String, name: String)
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    val path = "./src/main/resources/energyData.csv"
    data = FileManager.makeDataFrame(spark, path, true, ",")
//    val path = "./src/main/resources/2017-07.csv"
//    data = FileManager.makeDataFrame(spark, path, true, ",")
  }
  
  def incrOpId: Int = {
    opId += 1
    (opId)
  }
  
}
