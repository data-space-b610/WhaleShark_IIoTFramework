//package ksb.csle.component.operator.custom
//
//import scala.collection.JavaConversions._
//import org.apache.spark.sql.DataFrame
//
//import ksb.csle.common.base.operator.BaseDataOperator
//import ksb.csle.common.proto.StreamOperatorProto._
//
//// 새로 만든 메세지 객체 임포트
//import ksb.csle.common.proto.CustomOperatorProto.MySelectColumnsWithFileInfo
//import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
//
//class MyColumnSelectWithFileOperator(
//    o: StreamOperatorInfo
//    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {
//
//  private val p = o.getMySelectColumnsWithFile
//  private val path = p.getColumnIdPath
//  private var columnNames: Array[String] = null
//
//  private def selectColumns(df: DataFrame): DataFrame = {
//    logger.debug(s"OpId ${o.getId} : SelectColumns")
//
//    if (columnNames == null) {
//      columnNames = df.sparkSession.sparkContext.textFile(path)
//      .map(_.split(",")).collect.flatten
//    }
//    val result = df.select(columnNames.head, columnNames.tail:_*)
//
//    logger.debug(result.show.toString)
//    logger.debug(result.printSchema.toString)
//
//    result
//   }
//
//  override def operate(df: DataFrame): DataFrame = selectColumns(df)
//}
//
//object MyColumnSelectWithFileOperator {
//  def apply(o: StreamOperatorInfo): MyColumnSelectWithFileOperator =
//    new MyColumnSelectWithFileOperator(o)
//}
