package ksb.csle.component.reader

import scala.collection.JavaConversions._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.reader.BaseReader

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that loads multiple DataFrame from files and HBase tables
 * and then merge them under given conditions.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.MultipleReadersInfo]]
 *          MultipleReadersInfo contains attributes as follows:
 *          - fileInfo: Attributes to merge file data (required)
 *          - phoenixInfo: Attributes to merge HBase table (required)
 *          - joinColumnName: Column name for joining (optional)
 *
 * ==MultipleReadersInfo==
 * {{{
 * message MultipleReadersInfo {
 *   repeated FileMergingInfo fileInfo = 1;
 *   repeated PhoenixMergingInfo phoenixInfo = 2;
 *   optional string joinColumnName = 3 [default = "TIMESTAMP"];
 * }
 * }}}
 *
 *          [[ksb.csle.common.proto.DatasourceProto.FileMergingInfo]]
 *          FileMergingInfo contains attributes as follows:
 *          - fileInfo: Attributes to read file data (required)
 *                      Note: see 'FileReader' for more details
 *          - mergeType: Merge method (optional)
 *
 * ==FileMergingInfo==
 * {{{
 * message FileMergingInfo {
 *   required FileInfo fileInfo = 1;
 *   optional MergeType mergeType = 2 [default = INNER_JOIN];
 * }
 * }}}
 *
 *          [[ksb.csle.common.proto.DatasourceProto.PhoenixMergingInfo]]
 *          PhoenixMergingInfo contains attributes as follows:
 *          - phoenixInfo: Attributes to read HBase table (required)
 *                         Note: see 'PhoenixReader' for more details
 *          - mergeType: Merge method (optional)
 *
 * ==PhoenixMergingInfo==
 * {{{
 * message PhoenixMergingInfo {
 *   required PhoenixInfo phoenixInfo = 1;
 *   optional MergeType mergeType = 2 [default = INNER_JOIN];
 * }
 * }}}
 */
class MultipleReaders(
    val o: BatchReaderInfo
    ) extends BaseReader[
      DataFrame, BatchReaderInfo, SparkSession](o) with HBaseFuncAPI {

  val p: MultipleReadersInfo = o.getMultipleReaders

  /**
   * This reads DataFrame from HBase table using Apache Phoenix.
   *
   * @param spark SparkSession.
   * @return DataFrame.
   */
  override def read(spark: SparkSession): DataFrame = {
    import spark.implicits._

    var result: DataFrame = null

    p.getFileInfoList.toList.map(info =>
      result = merge(result, read(spark, info.getFileInfo),
          info.getMergeType))

    p.getPhoenixInfoList.toList.map(info =>
      result = merge(result, read(spark, info.getPhoenixInfo),
          info.getMergeType))

    logger.info(result.printSchema.toString)
    (result)
  }

  private def read(spark: SparkSession, info: FileInfo): DataFrame = {
    import spark.implicits._

    var df: DataFrame = null
    try {
      df = spark.read.format(info.getFileType.toString.toLowerCase)
            .option("sep", info.getDelimiter)
            .option("header", info.getHeader)
            .option("inferSchema", "true")
            .load(info.getFilePathList.toList.mkString(","))
            .cache
      logger.info(df.show.toString)
      logger.info(df.printSchema.toString)
    } catch {
      case e: ClassCastException =>
        logger.error(s"Unsupported type cast error: ${e.getMessage}")
      case e: UnsupportedOperationException =>
        logger.error(s"Unsupported file reading error: ${e.getMessage}")
      case e: Throwable =>
        logger.error(s"Unknown file reading error: ${e.getMessage}")
    }
    df
  }

  private def merge(
      df1: DataFrame,
      df2: DataFrame,
      mergingType: MergeType): DataFrame = {
    if (df1 == null) df2
    else {
      mergingType match {
        case MergeType.MERGING_APPEND => appendDF(df1, df2)
        case MergeType.MERGING_CONCAT => concatDF(df1, df2)
        case MergeType.INNER_JOIN =>
          innerJoin(df1, df2, p.getJoinColumnName)
        case MergeType.OUTER_JOIN =>
          outerJoin(df1, df2, p.getJoinColumnName)
      }
    }
  }

  private def appendDF(df1: DataFrame, df2: DataFrame): DataFrame = {
    require(df1.count == df2.count)

    val dfSeq1 = df1.collect.map(_.toSeq)
    val dfSeq2 = df2.collect.map(_.toSeq)

    var result = List[Row]()
    for(i <- 0 until df1.count.toInt)
      result = result.+:(Row.fromSeq(dfSeq1(i) ++ dfSeq2(i)))

    val schema = (df1.schema.map(x => new StructField(x.name, x.dataType))
               ++ df2.schema.map(x => new StructField(x.name, x.dataType)))
    val rdd = df1.sparkSession.sparkContext.parallelize(result)

    df1.sparkSession.createDataFrame(rdd, StructType(schema))
  }

  private def concatDF(df1: DataFrame, df2: DataFrame): DataFrame = {
    require(df1.columns.length == df2.columns.length)

//    df1.union(df2).distinct()
    // distinct function causes the unsolvable errors.
    df1.union(df2)
  }

  private def innerJoin(df1: DataFrame, df2: DataFrame,
      joinColumn: String): DataFrame = {
    df1.join(df2, Seq(joinColumn))
  }

  private def outerJoin(df1: DataFrame, df2: DataFrame,
      joinColumn: String): DataFrame = {
    df1.join(df2, df1(joinColumn) === df2(joinColumn), "left_outer")
  }

  /**
   * Close the MultipleReaders.
   */
  override def close {
  }
}

object MultipleReaders {
  def apply[P](o: BatchReaderInfo): MultipleReaders = new MultipleReaders(o)
}
