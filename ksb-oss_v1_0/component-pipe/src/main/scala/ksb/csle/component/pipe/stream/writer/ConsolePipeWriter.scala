package ksb.csle.component.pipe.stream.writer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.pipe.writer.BasePipeWriter

/**
 * :: Experimental ::
 *
 * Writer that saves DataFrame to a file.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.ConsolePipeWriterInfo]]
 *          ConsolePipeWriterInfo contains followed attributes:
 *          - mode: Write mode such as append, update, and complete
 *          - trigger: triggering interval
 *
 * ==ConsolePipeWriterInfo==
 * {{{
 *  message ConsolePipeWriterInfo {
 *   required string mode = 1 [default="append"];
 *   required string trigger = 2;
 *   }
 * }}}
 */
class ConsolePipeWriter(
    val o: StreamPipeWriterInfo,
    val session: SparkSession
    ) extends BasePipeWriter[
      DataFrame, StreamingQuery, StreamPipeWriterInfo, SparkSession](o, session) {

  val p: ConsolePipeWriterInfo = o.getConsolePipeWriter

  /**
   * Writes dataframe to file
   *
   * @param df Dataframe to write
   * @return handle StreamingQuery
   */
  override def write(df: DataFrame): StreamingQuery = {
    df.writeStream
      .outputMode(p.getMode)
      .format("console")
      .trigger(Trigger.ProcessingTime(p.getTrigger))
      .start()
  }

  override def close: Unit = ()
}
