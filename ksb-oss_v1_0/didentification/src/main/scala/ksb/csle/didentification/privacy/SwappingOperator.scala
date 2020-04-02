package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.Map
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.verification.Verification
import ksb.csle.didentification.utilities.FileManager

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the swapping module in the PseudoAnonymization
 * algorithm. Compared to heuristic module which makes the heuristic
 * table using randomized string or manually, this module makes the
 * heuristic table by referring to the given input file. 
 * It replaces the values of data with some random strings.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.SwappingInfo]]
 *          SwappingInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the swapping function
 *          - method: how to swap the record (randomly? or by referring to the record of the given file).
 *          - fileInfo: the configuration for the file-based swapping. 
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==SwappingInfo==
 *  {{{
 *  enum GenSwappingTableMethod {
 *  	SWAP_RANDOM = 0;
 *  	SWAP_FILE = 1;
 *  }
 *  enum FileLocationType {
 *  	DIRECTORY = 0;
 *  	URL = 1;
 *  }
 *  message SwappingFileInfo {
 *  	required string filePath = 1;
 *  	required int32 columnIndex = 2;
 *  	optional FileLocationType fileType = 3 [default = DIRECTORY];
 *  }
 *  message SwappingInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required GenSwappingTableMethod method = 2 [default = SWAP_RANDOM];
 *    optional SwappingFileInfo fileInfo = 3;
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class SwappingOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getSwapping.getCheck) {

  val p: SwappingInfo = o.getSwapping

  /**
   * Swaps the original records with pre-defined values. To do this,
   * it makes the swap tables on the basis of given file content.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be pseudo-anonymized
   * @param swappingType How to make swap lists
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame =
    anonymizeColumn(src, columnName, p.getMethod)

  def anonymizeColumn(
      src: DataFrame,
      columnName: String,
      swappingType: GenSwappingTableMethod): DataFrame = {
    val table = makeHeuristicTable(src, columnName, swappingType)
    def swappingAnonymize: (String => String) = {
      key => table.get(key) match {
        case Some(value) => value
        case _ => "None"
      }
    }
    val swappingUdf = udf(swappingAnonymize)
    val result = src.withColumn(columnName, swappingUdf(src.col(columnName)))

//    result.show
//    to check de-anonymize capability
//    deAnonymize(result, columnName, table).show
    (result)
  }

  /**
   * Makes swap tables comprising of [original data, random string].
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be pseudo-anonymized
   * @param swappingType How to make swap lists
   * @return Map[String, String] Generated heuristic table
   */
  def makeHeuristicTable(
      src: DataFrame,
      columnName: String,
      swappingType: GenSwappingTableMethod): Map[String, String] = {
    swappingType match {
      case GenSwappingTableMethod.SWAP_RANDOM =>
        makeFileHeuristicTable(src, columnName, p.getFileInfo)
      case GenSwappingTableMethod.SWAP_FILE =>
        makeFileHeuristicTable(src, columnName, p.getFileInfo)
    }
  }

  /**
   * Makes swap tables by referring to the given file information.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be pseudo-anonymized
   * @param fileInfo the information containing how to make swap tables
   * @return Map[String, String] Generated heuristic table
   */
  def makeFileHeuristicTable(
      src: DataFrame,
      columnName: String,
      fileInfo: SwappingFileInfo): Map[String, String] = {
    val heuristicDF =
      if(fileInfo.getFileType == FileLocationType.DIRECTORY)
        FileManager.makeDataFrame(src.sparkSession, fileInfo.getFilePath)
      else
        FileManager.makeDataFrameURL(src.sparkSession, fileInfo.getFilePath)
    val DFtoList = changeDFtoList(heuristicDF, fileInfo.getColumnIndex)

    val table = Map[String, String]()
    var index = 0
    src.select(src.col(columnName)).dropDuplicates()
      .rdd.zipWithUniqueId.collect.map(r => {
      val key = r._1.toString.dropRight(1).drop(1)
      table += (key -> DFtoList(index))
      index += 1
      if(index >= DFtoList.length) index = 0
    })
    (table)
  }

  private def changeDFtoList(
      df: DataFrame,
      colIndex: Int): List[String] =
    df.select(df.columns(colIndex)).dropDuplicates()
      .rdd.map(r => r.getString(0)).collect.toList

  /**
   * Operates heuristic module for basic de-identification
   *
   * @param runner BaseRunner to run
   * @param df Input dataframe
   * @return DataFrame Anonymized dataframe
   */
  override def operate(df: DataFrame): DataFrame = {
    val columnIDs: Array[Int] =
      (p.getSelectedColumnIdList map (_.asInstanceOf[Int])).toArray

    val result = anonymize(df, getColumnNames(df, columnIDs))
    
    val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    Verification(privacy).printAnonymizeResult(df, result, fieldInfos)   
    (result)
  }

}

object SwappingOperator {
  def apply(o: StreamOperatorInfo): SwappingOperator = new SwappingOperator(o)
}
