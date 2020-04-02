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
 * Writer that writes streaming data to in-memory.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.MemoryPipeWriter]]
 *          MemoryPipeWriter contains followed attributes:
 *          - mode: Write mode such as append, update, and complete
 *          - trigger: triggering interval
 *          - tableName: table name in in-memory cache
 *
 * ==MemoryPipeWriterInfo==
 * {{{
 *  message MemoryPipeWriterInfo {
 *   required string mode = 1 [default="append"];
 *   required string trigger = 2;
 *   required string tableName = 3;
 *   }
 * }}}
 */
class MemoryPipeWriter(
    val o: StreamPipeWriterInfo,
    val session: SparkSession
    ) extends BasePipeWriter[
      DataFrame, StreamingQuery, StreamPipeWriterInfo, SparkSession](o, session) {

  val p: MemoryPipeWriterInfo = o.getMemoryPipeWriter

  /**
   * Writes dataframe to memory
   *
   * @param df Dataframe to write
   * @return handle StreamingQuery
   */
  override def write(df: DataFrame): StreamingQuery = {
    df.writeStream
      .outputMode(p.getMode)
      .format("memory")
      .queryName(p.getTableName)
      .trigger(Trigger.ProcessingTime(p.getTrigger))
      .start()
  }

  override def close: Unit = ()
}
