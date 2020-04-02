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
class PrivacyDataTest extends UnitSpec 
  with DataSuite with Logging {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  @transient var data: DataFrame = _
  var opId: Int = 0

  case class DefaultColumnInfo(id: Int, colType: String, name: String)
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    val path = "./src/main/resources/adult.csv"
    data = FileManager.makeDataFrame(spark, path, true, ";", makeDefaultSchema)
//    val path = "./src/main/resources/2017-07.csv"
//    data = FileManager.makeDataFrame(spark, path, true, ",")
  }
  
  def incrOpId: Int = {
    opId += 1
    (opId)
  }

  private def makeDefaultSchema(): StructType = {
    var schemaList = List[DefaultColumnInfo]()
    schemaList = schemaList :+ new DefaultColumnInfo(0, "String", "sex")
    schemaList = schemaList :+ new DefaultColumnInfo(1, "Integer", "age")
    schemaList = schemaList :+ new DefaultColumnInfo(2, "String", "race")
    schemaList = schemaList :+ new DefaultColumnInfo(3, "String", "marital-status")
    schemaList = schemaList :+ new DefaultColumnInfo(4, "String", "education")
    schemaList = schemaList :+ new DefaultColumnInfo(5, "String", "native-country")
    schemaList = schemaList :+ new DefaultColumnInfo(6, "String", "workclass")
    schemaList = schemaList :+ new DefaultColumnInfo(7, "String", "occupation")
    schemaList = schemaList :+ new DefaultColumnInfo(8, "String", "salary-class")
    StructType(schemaList.map(toStructField(_)).toSeq)
  }
  
  private def toStructField(
      colInfo: DefaultColumnInfo): StructField = {
    if(colInfo.colType == "String") 
        new StructField(colInfo.name, StringType)
    else  
        new StructField(colInfo.name, IntegerType)
  }  
  
  def getBasePrivacyInfo(): PrivacyCheckInfo = {
    PrivacyCheckInfo.newBuilder()
      .setMeasureLoss(LossMeasureMethod.AECS)
      .setMeasureRisk(RiskMeasureMethod.UNIQUENESS)
      .setCheckAnonymity(CheckAnonymityMethod.KANONYMITYCHECK)
      .build()
  }
  
  def makeFieldInfo(
      key: String, 
      fieldType: FieldType,
      attrType: FieldInfo.AttrType): FieldInfo = {
    FieldInfo.newBuilder()
      .setKey(key)
      .setType(fieldType)
      .setAttrType(attrType)
      .build()
  }
   
}
