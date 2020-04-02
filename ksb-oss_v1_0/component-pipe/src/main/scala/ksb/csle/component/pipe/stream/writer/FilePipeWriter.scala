package ksb.csle.component.pipe.stream.writer

import scala.collection.JavaConversions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.pipe.writer.BasePipeWriter

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that saves DataFrame to a file.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.FilePipeWriterInfo]]
 *          FilePipeWriterInfo contains followed attributes:
 *          - fileInfo: FileInfo contains attributes as follows:
 *            - filePath: File path where writes records (required)
 *            - fileType: File type (optional)
 *            - delimiter: Delimiter of each field (optional)
 *            - field: Schema of input record (optional)
 *            - header: True if has a header line (optional)
 *          - mode: Write mode such as append, update, and complete
 *          - trigger: triggering interval
 *          - chechpointLocation: checkpoint file path
 *
 * ==FilePipeWriterInfo==
 * {{{
 *  message FilePipeWriterInfo {
 *   required FileInfo fileInfo = 1;
 *   optional string mode = 2 [default="append"];
 *   optional string trigger = 3;
 *   required string checkpointLocation = 4 [default="/checkpoint/file"];
 *  }
 * }}}
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
class FilePipeWriter(
    val o: StreamPipeWriterInfo,
    val session: SparkSession
    ) extends BasePipeWriter[
      DataFrame, StreamingQuery, StreamPipeWriterInfo, SparkSession](o, session) {

  private val p: FilePipeWriterInfo = o.getFilePipeWriter
  private val outFormat = p.getFileInfo.getFileType match {
    case FileInfo.FileType.CSV => "csv"
    case FileInfo.FileType.JSON => "json"
    case FileInfo.FileType.PARQUET => "parquet"
    case _ => "csv"
  }

  /**
   * Writes dataframe to file
   *
   * @param df Dataframe to write
   * @return handle StreamingQuery
   */
  // TODO: Occcasionally, it causes the error "_spark_metadata does not exist"
  override def write(df: DataFrame): StreamingQuery = {
    logger.info(s"OpId ${o.getId} : FilePipeWriter")
    df.printSchema()

  // spark ml inference may add the new column which is the type of vector.
  // note that the type of vector cannot be written in a csv file.
    var result = df
    df.schema.map(col =>
      if(col.dataType == org.apache.spark.ml.linalg.SQLDataTypes.VectorType)
        result = result.withColumn(col.name, to_json(struct(df(col.name))))
    )

    result
      .writeStream
      .format(outFormat)
      .option("path", p.getFileInfo.getFilePath(0))
      .option("checkpointLocation", p.getCheckpointLocation)
      .trigger(Trigger.ProcessingTime(p.getTrigger))
      .outputMode(p.getMode)
      .start()
  }

  private def getFileFormat(format: FileInfo.FileType): String = format match {
    case FileInfo.FileType.CSV => "csv"
    case FileInfo.FileType.JSON => "json"
    case FileInfo.FileType.PARQUET => "parquet"
    case _ => "csv"
  }

  override def close: Unit = ()
}
