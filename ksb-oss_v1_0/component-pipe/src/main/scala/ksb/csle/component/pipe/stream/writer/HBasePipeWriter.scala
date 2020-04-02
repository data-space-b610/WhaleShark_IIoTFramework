package ksb.csle.component.pipe.stream.writer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.OutputMode
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.pipe.writer.BasePipeWriter

/**
 * :: Experimental ::
 *
 * Writer that writes streaming data to HBase.
 */
class HBasePipeWriter(
    val o: StreamPipeWriterInfo,
    val session: SparkSession
    ) extends BasePipeWriter[
      DataFrame, StreamingQuery, StreamPipeWriterInfo, SparkSession](o, session) {

  val p: HBasePipeWriterInfo = o.getHbasePipeWriter

  override def write(df: DataFrame): StreamingQuery = {
    logger.info(s"OpId ${o.getId} : HBasePipeWriter")
    logger.info(df.printSchema().toString())
    df.writeStream
      .queryName("hbase writer")
      .format("org.apache.spark.sql.execution.datasources.hbase.HBaseSinkProvider")
      .outputMode(p.getMode)
      .trigger(Trigger.ProcessingTime(p.getTrigger))
      .option("checkpointLocation", p.getCheckpointLocation)
      .option("hbasecat", p.getCatalog)
      .start()
  }

  override def close: Unit = ()
}
