package ksb.csle.component.writer

import java.io.File

import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext

import org.apache.hadoop.fs.FileUtil

import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FileStreamWriterInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that saves DataFrame to a file.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.FileStreamWriterInfo]]
 *          FileStreamWriterInfo contains attributes as follows:
 *          - filePath: file path where writes records (required)
 *          - fileType: file type, default is 'CSV' (optional)
 *          - header: true if has a header line (optional)
 *          - saveMode: specify the expected behavior of saving a DataFrame to a data source, default is 'OVERWRITE' (optional)
 *
 * ==FileInfo==
 * {{{
 * message FileStreamWriterInfo {
 *   enum FileType {
 *       CSV = 0;
 *       JSON = 1;
 *       PARQUET = 2;
 *       TEXT = 3;
 *   }
 *
 *   enum SaveMode {
 *       APPEND = 0;
 *       OVERWRITE = 1;
 *       ERROR_IF_EXISTS = 2;
 *       IGNORE = 3;
 *   }
 *
 *   required string filePath = 1;
 *   optional FileType fileType = 2 [default = CSV];
 *   optional bool header = 3 [default = true];
 *   optional SaveMode saveMode = 4 [default = OVERWRITE];
 * }
 * }}}
 */
class FileStreamWriter(
    val o: StreamWriterInfo
    ) extends BaseWriter[DataFrame, StreamWriterInfo, StreamingContext](o) {

  private[this] val info =
    if (o.getFileStreamWriter == null) {
      throw new IllegalArgumentException("FileStreamWriterInfo is not set.")
    } else {
      o.getFileStreamWriter
    }

  private[this] val pathBuilder = info.getFilePath match {
    case null | "" =>
      throw new IllegalArgumentException("filePath is not set.")
    case _ =>
      PathBuilderFactory.create(info.getFilePath)
  }

  /**
   * Save DataFrame to the file.
   *
   * @param d DataFrame to save
   */
  override def write(df: DataFrame): Unit = {
    val dstFilePath = pathBuilder.build
    if (info.getSaveMode == FileStreamWriterInfo.SaveMode.OVERWRITE) {
      val dstFile = new File(dstFilePath)
      if (dstFile.exists()) {
        dstFile.delete()
      }
    }

    val writer = createWriter(df)
    val tmpDirPath = buildTmpDirPath(dstFilePath)
    writer.save(tmpDirPath)

    val deleteSource =
      if (info.getSaveMode == FileStreamWriterInfo.SaveMode.APPEND) {
        false
      } else {
        true
      }
    merge(tmpDirPath, dstFilePath)
  }

  private def createWriter(df: DataFrame): DataFrameWriter[Row] = {
    val saveMode = info.getSaveMode match {
      case FileStreamWriterInfo.SaveMode.APPEND =>
        SaveMode.Append
      case FileStreamWriterInfo.SaveMode.OVERWRITE =>
        SaveMode.Overwrite
      case FileStreamWriterInfo.SaveMode.ERROR_IF_EXISTS =>
        SaveMode.ErrorIfExists
      case FileStreamWriterInfo.SaveMode.IGNORE =>
        SaveMode.Ignore
    }

    val header = info.getHeader

    val format = info.getFileType.name().toLowerCase()

    df.coalesce(1)
      .write
      .mode(saveMode)
      .option("header", header)
      .format(format)
  }

  private def buildTmpDirPath(destFilePath: String): String = {
    s"$destFilePath.tmp"
  }

  private def merge(srcPath: String, dstPath: String,
      deleteSource: Boolean = true): Unit = {
    scala.reflect.io.Path.string2path(dstPath).delete()
    val srcDir = new org.apache.hadoop.fs.Path(srcPath)
    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = srcDir.getFileSystem(conf)
    val dstFile = new org.apache.hadoop.fs.Path(dstPath)
    FileUtil.copyMerge(fs, srcDir, fs, dstFile, deleteSource, conf, null)
  }

  /**
   * Close the FileWriter.
   */
  override def close: Unit = {
  }

  private trait PathBuilder {
    def build: String
  }

  private object PathBuilderFactory {
    def create(filePath: String): PathBuilder = {
      if (filePath.indexOf('$') < 0) {
        new StaticPathBuilder(filePath)
      } else {
        new TimeseriesPathBuilder(filePath)
      }
    }
  }

  private class StaticPathBuilder(filePath: String) extends PathBuilder {
    def build: String = {
      filePath
    }
  }

  private class TimeseriesPathBuilder(filePath: String) extends PathBuilder {
    import java.util.Date
    import java.text.SimpleDateFormat

    private[this] val (prefix, pattern, suffix) = {
      val patCharIndex = filePath.indexOf('$')
      val patLeftIndex = filePath.indexOf('{', patCharIndex)
      val patRightIndex = filePath.indexOf('}', patCharIndex)

      if (patLeftIndex > 0 && patRightIndex > 0) {
        (filePath.substring(0, patCharIndex),
            filePath.substring(patLeftIndex + 1, patRightIndex),
            filePath.substring(patRightIndex + 1))
      } else {
        throw new IllegalArgumentException("filePath is not valid.")
      }
    }

    def build: String = {
      val path = new StringBuilder(prefix)

      val dateTime = new SimpleDateFormat(pattern).format(new Date())
      path.append(dateTime)

      path.append(suffix).toString()
    }
  }
}
