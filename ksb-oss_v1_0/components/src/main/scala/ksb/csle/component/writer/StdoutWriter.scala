package ksb.csle.component.writer

import org.apache.spark.sql._

import ksb.csle.common.proto.DatasourceProto.{WriterInfo, BatchWriterInfo}
import ksb.csle.common.base.writer.BaseWriter

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that print the schema and some rows of DataFrame to standard output.
 *
 * @tparam p Any type.
 */
class StdoutWriter[P](
    val p: P) extends BaseWriter[DataFrame, P, SparkSession](p) {

  /**
   * Print the schema and some rows of DataFrame to standard output.
   */
  override def write(d: DataFrame): Unit = {
    d.printSchema()
    d.show(10)
  }

  /**
   * Close the StdoutWriter.
   */
  override def close: Unit = ()
}

object StdoutWriter {
  def apply[P](p: P): StdoutWriter[P] = new StdoutWriter(p)
}
