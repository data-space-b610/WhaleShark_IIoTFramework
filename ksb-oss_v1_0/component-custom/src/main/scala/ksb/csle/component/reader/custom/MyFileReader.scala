//package ksb.csle.component.reader.custom
//
//import scala.collection.JavaConversions._
//import java.io._
//
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.types._
//import ksb.csle.common.base.reader.BaseReader
//import ksb.csle.common.utils.SparkUtils.getSchema
//import ksb.csle.common.proto.DatasourceProto.BatchReaderInfo
//import ksb.csle.common.proto.CustomDatasourceProto.MyFileInfo
//import ksb.csle.common.proto.CustomDatasourceProto.MyFileInfo.FieldInfo
//
//class MyFileReader(
//    val o: BatchReaderInfo
//    ) extends BaseReader[DataFrame, BatchReaderInfo, SparkSession](o) {
//
//  private val myFileReaderInfo: MyFileInfo = o.getMyFileReader
//
//  // TODO: Adds all kinds of type casting.
//  def getSchema(ts: List[FieldInfo]): StructType =
//    StructType(
//      ts.map { t =>
//        t.getType match {
//          case FieldInfo.FieldType.INTEGER =>
//            StructField(t.getKey, IntegerType, true)
//          case FieldInfo.FieldType.STRING =>
//            StructField(t.getKey, StringType, true)
//          case FieldInfo.FieldType.DOUBLE =>
//            StructField(t.getKey, DoubleType, true)
//          case FieldInfo.FieldType.BOOLEAN =>
//            StructField(t.getKey, BooleanType, true)
//          case FieldInfo.FieldType.BYTE =>
//            StructField(t.getKey, ByteType, true)
//          case FieldInfo.FieldType.FLOAT =>
//            StructField(t.getKey, FloatType, true)
//          case FieldInfo.FieldType.LONG =>
//            StructField(t.getKey, LongType, true)
//          case FieldInfo.FieldType.TIMESTAMP =>
//            StructField(t.getKey, TimestampType, true)
//          case _ =>
//            StructField(t.getKey, StringType, true)
//        }
//      })
//
//  override def read(spark: SparkSession): DataFrame = {
//    import spark.implicits._
//    try {
//      logger.debug("Operation: MyFileReader")
//      logger.debug(s"FilePath: ${myFileReaderInfo.getFilePathList.toList.mkString(",")}")
//
//      if(myFileReaderInfo.getFileType.toString().toLowerCase() != "csv"){
//         if (myFileReaderInfo.getFieldList.toList.size() > 0) {
//            spark.sqlContext.read
//              .format(myFileReaderInfo.getFileType.toString().toLowerCase())
//              .option("header", myFileReaderInfo.getHeader()) // Use first line of all files as header
//              .option("sep", myFileReaderInfo.getDelimiter)
//              .schema(getSchema(myFileReaderInfo.getFieldList.toList))
//              .load(myFileReaderInfo.getFilePathList.toList.mkString(","))
//         }else{
//            spark.sqlContext.read
//              .format(myFileReaderInfo.getFileType.toString().toLowerCase())
//              .option("header", myFileReaderInfo.getHeader()) // Use first line of all files as header
//              .option("inferSchema", "true")
//              .option("sep", myFileReaderInfo.getDelimiter)
//              .load(myFileReaderInfo.getFilePathList.toList.mkString(","))
//         }
//      }else{
//         if (myFileReaderInfo.getFieldList.toList.size() > 0) {
//          spark.read
//          .option("header", myFileReaderInfo.getHeader())
//          .option("sep", myFileReaderInfo.getDelimiter)
//          .schema(getSchema(myFileReaderInfo.getFieldList.toList))
//          .csv(myFileReaderInfo.getFilePathList.toList.mkString(","))
//         }else{
//          spark.read
//          .option("header", myFileReaderInfo.getHeader())
//          .option("inferSchema", "true")
//          .option("sep", myFileReaderInfo.getDelimiter)
//          .csv(myFileReaderInfo.getFilePathList.toList.mkString(","))
//      }
//   }
//    } catch {
//      case e: ClassCastException =>
//        logger.error(s"Unsupported type cast error: ${e.getMessage}")
//        throw e
//      case e: UnsupportedOperationException =>
//        logger.error(s"Unsupported file reading error: ${e.getMessage}")
//        throw e
//      case e: Throwable =>
//        logger.error(s"Unknown file reading error: ${e.getMessage}")
//        throw e
//    }
//  }
//
//  override def close: Unit = ()
//}
//
//object MyFileReader {
//  def apply(o: BatchReaderInfo): MyFileReader = new MyFileReader(o)
//}
