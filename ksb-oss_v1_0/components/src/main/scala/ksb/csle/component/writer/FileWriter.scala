package ksb.csle.component.writer

import java.io._

import scala.collection.JavaConversions._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.types._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.utils.SparkUtils.getSchema

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that saves DataFrame to a file.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.FileInfo]]
 *          FileInfo contains attributes as follows:
 *          - filePath: File path where writes records (required)
 *          - fileType: File type (optional)
 *          - delimiter: Delimiter of each field (optional)
 *          - field: Schema of input record (optional)
 *          - header: True if has a header line (optional)
 *          - saveMode: specify the expected behavior of saving a DataFrame to a data source. (optional)
 *
 * ==FileInfo==
 * {{{
 * message FileInfo {
 *   repeated string filePath = 1;
 *   optional FileType fileType = 2 [default = JSON];
 *   optional string delimiter = 3 [default = ","];
 *   repeated FieldInfo field = 4;
 *   optional bool header = 5 [default = false];
 *   optional SaveMode saveMode = 6 [default = OVERWRITE];
 *   enum FileType {
 *     CSV = 0;
 *     JSON = 1;
 *     PARQUET = 2;
 *     TEXT = 3;
 *   }
 *   enum SaveMode {
 *     APPEND = 0;
 *     OVERWRITE = 1;
 *     ERROR_IF_EXISTS = 2;
 *    IGNORE = 3;
 *   }
 * }
 * }}}
 */
class FileWriter(
    val o: BatchWriterInfo
    ) extends BaseWriter[DataFrame, BatchWriterInfo, SparkSession](o) {

  private[csle] val fileWriterInfo: FileInfo = o.getFileWriter

  private def merge(srcPath: String, dstPath: String,
      deleteSource: Boolean): Unit = {
    scala.reflect.io.Path.string2path(dstPath).delete()
    val srcDir = new org.apache.hadoop.fs.Path(srcPath)
    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = srcDir.getFileSystem(conf)
    val dstFile = new org.apache.hadoop.fs.Path(dstPath)
    if(fs.exists(dstFile)) {
      fs.delete(dstFile, true)
    }
    org.apache.hadoop.fs.FileUtil.copyMerge(fs, srcDir, fs, dstFile,
        deleteSource, conf, null)
  }

 /**
   * Save DataFrame to the file.
   *
   * @param d DataFrame to save
   */
  override def write(d: DataFrame): Unit = {
    logger.info("Operation : FileWriter")
    val tmpPathStr = fileWriterInfo.getFilePath(0) + "_temp"
    val tmpPath =
    try {
       new File(new java.net.URI(tmpPathStr))
    } catch {
      case e: Exception =>
        new File(tmpPathStr)
    }

    val mode = fileWriterInfo.getSaveMode match {
      case FileInfo.SaveMode.APPEND => SaveMode.Append
      case FileInfo.SaveMode.ERROR_IF_EXISTS => SaveMode.ErrorIfExists
      case FileInfo.SaveMode.IGNORE => SaveMode.Ignore
      case _ => SaveMode.Overwrite
    }

//    if (tmpPath.exists()) FileUtils.deleteDirectory(tmpPath)
    val isHeader = if (tmpPath.exists() && mode == SaveMode.Append) false
                   else fileWriterInfo.getHeader()

    if (fileWriterInfo.getFileType == FileInfo.FileType.CSV)
        d.coalesce(1)
         .write
         .mode(mode)
         .option("header", isHeader)
         .csv(tmpPathStr)
//      d.coalesce(1).write.csv(srcPath)
    if (fileWriterInfo.getFileType == FileInfo.FileType.PARQUET)
      d.coalesce(1)
       .write
       .mode(mode)
       .parquet(tmpPathStr)
    if (fileWriterInfo.getFileType == FileInfo.FileType.JSON)
      d.coalesce(1)
       .write
       .mode(mode)
       .json(tmpPathStr)
    if (fileWriterInfo.getFileType == FileInfo.FileType.TEXT)
      d.coalesce(1)
       .write
       .mode(mode)
       .text(tmpPathStr)

    val dstPath =
      try {
         new File(new java.net.URI(fileWriterInfo.getFilePath(0)))
      } catch {
        case e: Exception =>
          new File(fileWriterInfo.getFilePath(0))
      }
      
    if (dstPath.exists()) FileUtils.deleteQuietly(dstPath)

    val deleteSource = (mode != SaveMode.Append)
    merge(tmpPathStr, fileWriterInfo.getFilePath(0), deleteSource)
  }

  /**
   * Close the FileWriter.
   */
  override def close: Unit = ()
}

object FileWriter {
  def apply(o: BatchWriterInfo): FileWriter = new FileWriter(o)
}
