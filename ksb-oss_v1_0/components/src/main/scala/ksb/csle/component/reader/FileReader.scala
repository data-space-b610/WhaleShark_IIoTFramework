package ksb.csle.component.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.utils.SparkUtils.getSchema
import ksb.csle.common.proto.DatasourceProto.BatchReaderInfo
import ksb.csle.common.proto.DatasourceProto.FileInfo
import scala.collection.JavaConversions._
import java.io._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that loads records from one or more files.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.FileInfo]]
 *          FileInfo contains attributes as follows:
 *          - filePath: List of data file path (required)
 *          - fileType: File type (optional)
 *          - delimiter: Delimiter of each field (optional)
 *          - field: Schema of input record (optional)
 *          - header: True if has a header line (optional)
 *
 * ==FileInfo==
 * {{{
 * message FileInfo {
 *   repeated string filePath = 1;
 *   optional FileType fileType = 2 [default = JSON];
 *   optional string delimiter = 3 [default = ","];
 *   repeated FieldInfo field = 4;
 *   optional bool header = 5 [default = false];
 *   enum FileType {
 *     CSV = 0;
 *     JSON = 1;
 *     PARQUET = 2;
 *     TEXT = 3;
 *   }
 * }
 * }}}
 */
class FileReader(
    val o: BatchReaderInfo
    ) extends BaseReader[DataFrame, BatchReaderInfo, SparkSession](o) {

  private[csle] val fileReaderInfo: FileInfo = o.getFileReader

  /**
   * Load records in as DataFrame from one or more files.
   *
   * @param  session   Spark session
   * @return DataFrame DataFrame read from one or more files.
   */
  override def read(spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      logger.info("Operation: FileReader")
//      if (p.getFieldList.toList.size() > 0) {
//        spark.sqlContext.read
//          .format(p.getFileType.toString().toLowerCase())
//          .option("header", p.getHeader()) // Use first line of all files as header
//          .option("sep", p.getDelimiter)
//          .schema(getSchema(p.getFieldList.toList))
//          .load(p.getFilePathList.toList.mkString(","))
//      } else {
//        spark.sqlContext.read
//          .format(p.getFileType.toString().toLowerCase())
//          .option("header", p.getHeader())
//          .option("sep", p.getDelimiter)
//          .option("inferSchema", "true")
//          .load(p.getFilePathList.toList.mkString(","))
//      }

      if(fileReaderInfo.getFileType.toString().toLowerCase() != "csv"){
         if (fileReaderInfo.getFieldList.toList.size() > 0) {
            spark.sqlContext.read
              .format(fileReaderInfo.getFileType.toString().toLowerCase())
              .option("header", fileReaderInfo.getHeader()) // Use first line of all files as header
              .option("sep", fileReaderInfo.getDelimiter)
              .schema(getSchema(fileReaderInfo.getFieldList.toList))
              .load(fileReaderInfo.getFilePathList.toList.mkString(","))
         }else{
            spark.sqlContext.read
              .format(fileReaderInfo.getFileType.toString().toLowerCase())
              .option("header", fileReaderInfo.getHeader()) // Use first line of all files as header
              .option("inferSchema", "true")
              .option("sep", fileReaderInfo.getDelimiter)
              .load(fileReaderInfo.getFilePathList.toList.mkString(","))
         }
      }else{
         if (fileReaderInfo.getFieldList.toList.size() > 0) {
          spark.read
          .option("header", fileReaderInfo.getHeader())
          .option("sep", fileReaderInfo.getDelimiter)
          .schema(getSchema(fileReaderInfo.getFieldList.toList))
          .csv(fileReaderInfo.getFilePathList.toList.mkString(","))
         }else{
          spark.read
          .option("header", fileReaderInfo.getHeader())
          .option("inferSchema", "true")
          .option("sep", fileReaderInfo.getDelimiter)
          .csv(fileReaderInfo.getFilePathList.toList.mkString(","))
      }
   }
    } catch {
      case e: ClassCastException =>
        logger.error(s"Unsupported type cast error: ${e.getMessage}")
        throw e
      case e: UnsupportedOperationException =>
        logger.error(s"Unsupported file reading error: ${e.getMessage}")
        throw e
      case e: Throwable =>
        logger.error(s"Unknown file reading error: ${e.getMessage}")
        throw e
    }
  }

  /**
   * Close the FileReader.
   */
  override def close: Unit = ()
}

object FileReader {
  def apply(o: BatchReaderInfo): FileReader = new FileReader(o)
}
