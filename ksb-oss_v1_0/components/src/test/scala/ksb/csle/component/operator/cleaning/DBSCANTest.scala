package ksb.csle.component.operator.cleaning

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.StreamOperatorProto.DBSCANInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.UnitSpec
import ksb.csle.component.operator.DataFunSuite
import org.scalatest._
import scala.reflect.runtime.universe
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry.FunctionType
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry
import org.apache.log4j.Level
import org.apache.log4j.Logger
/**
 * Test Class for DBSCAN function in data clustering package.
 */
case class DBSCANData(data1: Double, data2: Double)

class DBSCANTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[DBSCANData] = _
  @transient var correctAnswer: Array[Integer] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
 override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
      DBSCANData(6.848934849, 3.158069218),
      DBSCANData(-0.943968337, 22.91433149),
      DBSCANData(-1.175376171, 23.9929549),
      DBSCANData(-0.728804819, 23.59998941),
      DBSCANData(-0.554106973, 23.14728525),
      DBSCANData(-0.50858251, 23.55578863),
      DBSCANData(-0.655966008, 24.12991918),
      DBSCANData(-0.828753893, 23.06295102),
      DBSCANData(-0.906446078, 23.63442066),
      DBSCANData(-1.175471759, 23.23088862),
      DBSCANData(-0.586424383, 23.56802483),
      DBSCANData(6.029774804, 3.337247273),
      DBSCANData(-1.022162431, 23.21138139),
      DBSCANData(-0.665984656, 23.20667453),
      DBSCANData(-0.578946901, 23.40512492),
      DBSCANData(-0.45042623, 23.88963325),
      DBSCANData(-0.639808699, 23.55207991),
      DBSCANData(-0.971141606, 23.10933188),
      DBSCANData(-0.866241774, 22.74841298),
      DBSCANData(-0.751035994, 22.83455997))
    correctAnswer = Array(
        0, 1, 0, 1, 1, 1, 0, 1, 1, 1,
        1, 0, 1, 1, 1, 1, 1, 1, 1, 1)
  }

  def convertDF(data: Seq[DBSCANData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF.repartition(1)
    df
  }

  def convertArrayToDataframeWithIndex(
    input: Array[Integer],
    name: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df: DataFrame =
      spark.createDataFrame(
          input.zipWithIndex.map{case (value, idx) => (value, idx + 1)})
        .toDF(name, "INDEX")
    df
  }

  "A DBSCAN" should
      "do DBSCAN with EQUAL condition" in {
    val dBSCANInfo =
        DBSCANInfo.newBuilder()
        .setEps(0.5)
        .setMinPts(5)
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.leaning.DBSCAN")
      .setDBSCAN(dBSCANInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertArrayToDataframeWithIndex(correctAnswer, "correct_answer")
    val dBSCAN = DBScanOperator(operator1)
    var desDF =
      dBSCAN.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select("CLUSTER", "correct_answer").collect().foreach{
      case Row(x: Integer, y: Integer) => assert(
          x === y, "The feature value is not correct after EMClustering.")
    }
    resultDF.show
  }
}
