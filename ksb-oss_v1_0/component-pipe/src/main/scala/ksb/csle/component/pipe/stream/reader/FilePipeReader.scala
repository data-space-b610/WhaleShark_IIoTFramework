package ksb.csle.component.pipe.stream.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamReader

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.pipe.reader.BasePipeReader
import ksb.csle.common.utils.SparkUtils.getSchema
import scala.collection.JavaConversions._
import java.io._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that reads dataframe from file source.
 *
 * @param o Object that contains  message
 *          [[FilePipeReaderInfo]]
 *          FilePipeReaderInfo contains attributes as follows:
 *          - filePath: List of data file path (required)
 *          - fileType: File type (optional)
 *          - maxFilesPerTrigger: The maximum number of new files
 *                                to be considered in every trigger.
 *          - timeColName: Column name containing time information (required)
 *          - watermark: Time slot in seconds or minutest
 *                       The event time column and the threshold on
 *                       how late the data is expected to be
 *                       in terms of event time.
 *          - field: Field information (repeated)
 *          - delimiter: delimiter, default value is ',' (optional)
 *          - header: existance of header (optional)
 *
 *  ==FilePipeReaderInfo==
 *  {{{
 *  message FilePipeReaderInfo {
 *   required string filePath = 1;
 *   required FileType fileType = 2 [default = JSON];
 *   optional int32 maxFilesPerTrigger = 3;
 *   optional string timeColName = 5 [default = "timestamp"];
 *   optional string watermark = 6 [default = "1 seconds"];
 *   repeated FieldInfo field = 7;
 *   optional string delimiter = 8 [default = ","];
 *   optional bool header = 9 [default = false];
 *
 *   enum FileType {
 *    CSV = 0;
 *    JSON = 1;
 *    PARQUET = 2;
 *    TEXT = 3;
 *   }
 *  }
 *  }}}
 */
class FilePipeReader(
    val o: StreamPipeReaderInfo,
    val session: SparkSession
    ) extends BasePipeReader[DataFrame, StreamPipeReaderInfo, SparkSession](
        o, session) {

  import session.implicits._

  val p: FilePipeReaderInfo = o.getFilePipeReader
  val fileType: String = p.getFileType.toString().toLowerCase()
  val maxFiles = p.getMaxFilesPerTrigger

  val userSchema =
    if (p.getFieldCount > 0)
      getSchema(p.getFieldList.toList)
    else
      new StructType().add("value", "string")

  val streamReader: org.apache.spark.sql.streaming.DataStreamReader = session
    .readStream
    .format(fileType)

  // TODO: Define and use parameters to set watermark and timestamp column.
  override def read: DataFrame = {
      if( maxFiles > 0 ) {
        streamReader.option("maxFilesPerTrigger", maxFiles)
       }

      if (fileType == "csv") {
        streamReader.option("header", p.getHeader)
          .option("sep", p.getDelimiter)
          .schema(userSchema)
      }
      if (fileType == "json") {
        streamReader.schema(userSchema)
      }

//     streamReader
//       .load(p.getPath)
//       .withColumn(p.getTimeColName, current_timestamp())
//       .withWatermark(p.getTimeColName, p.getWatermark)

     val df = streamReader.load(p.getFilePath)

     val cond = p.getWatermark.contains("seconds") || p.getWatermark.contains("minutes")
     val rst =
       if (cond) {
         df.withWatermark(p.getTimeColName, p.getWatermark)
       }
       else {
         df
       }

     rst

  }

  override def close: Unit = ()
}
