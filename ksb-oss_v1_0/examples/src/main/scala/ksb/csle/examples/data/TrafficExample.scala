package ksb.csle.examples.data

import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.SharedProto._
import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.tools.client._
import ksb.csle.common.utils.config.ConfigUtils

/**
 * Object to create the data source test workflow as a protobuf
 * message WorkflowInfo.
 * See {@link WorkflowInfo}.
 * This is used for test of parquet data source workflow scenario.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object TrafficExample extends Logging {
  val appId: String = "Data-TrafficExample"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("localhost", 19999)
    client.submit(workflowJson)
    client.close()
  }

  private def workflow = {
    val removeSelectedColInfo = RemoveSelectedColumnsInfo.newBuilder()
      .addSelectedColumnId(7)
      .build()
    val concatInfo = ReshapeWithConcatInfo.newBuilder()
      .addSelectedColumnId(0)
      .addSelectedColumnId(1)
      .addSelectedColumnId(2)
      .addSelectedColumnId(3)
      .addSelectedColumnId(4)
      .setDelimiter("_")
      .setValueColName("DATE_TIME")
      .setCondition(ReshapeWithConcatInfo.Condition.KEEP_ORIGINAL_AND_RESULT)
      .build
    val SQL_FILTER: String = "< '2015_09_01_05_35' ORDER BY "
//    val SQL_FILTER: String = "DATE_TIME < '2015_09_01_05_35' ORDER BY DATE_TIME"
    val filterUsingSqlInfo = FilterUsingSqlInfo.newBuilder()
      .setSelectedColumnId(7)
      .addSubParam(
        SubParameter.newBuilder
          .setKey("sql_where")
          .setValue(SQL_FILTER)
          .build
        )
      .build
    val maxMinScalingInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(6)
      .setMax("0.5")
      .setMin("-0.5")
      .setWithMinMaxRange(true)
      .setMaxRealValue("100")
      .setMinRealValue("0")
      .build
    val pivotInfo = PivotInfo.newBuilder()
      .setSelectedColumnId(5)
      .setGroupByColumn("7")
      .setValueColumn("6")
      .setMethod(PivotInfo.Method.AVG)
      .build
    val mviInfo = MissingValueImputationInfo.newBuilder()
      .setScope(MissingValueImputationInfo.Scope.SCOPE_ALL)
      .addSelectedColumnId(1)
      .setMethod(MissingValueImputationInfo.Method.SPECIFIC_VALUE)
      .setHow(MissingValueImputationInfo.How.HOW_ANY)
      .addSubParam(
        SubParameter.newBuilder
          .setKey("numeric")
          .setValue("0")
          .build
          )
      .build
    val addTimeIndexInfo = AddTimeIndexColumnInfo.newBuilder()
      .setUserTimeIndexColumnId(0)
      .setUserTimeIndexPattern("yyyy_MM_dd_HH_mm")
      .build
    val aggregateTimeWindowInfo = AggregateTimeWindowInfo.newBuilder()
      .setScope(AggregateTimeWindowInfo.Scope.SCOPE_SELECTED)
      .addSelectedColumnId(1)
      .addSelectedColumnId(2)
      .addSelectedColumnId(3)
      .addSelectedColumnId(4)
      .addSelectedColumnId(5)
      .addSelectedColumnId(6)
      .addSelectedColumnId(7)
      .addSubParam(
        SubParameter.newBuilder
          .setKey("minute")
          .setValue("10")
          .build
          )
      .build
    val sCol =
      "1000000300,1000000100,1000000200,1000000400,1000000500,1000000600,1000000700"
    val selectedColumnsInfo = SelectColumnsInfo.newBuilder()
      .addSelectedColumnId(1)
      .addSelectedColumnId(4)
      .addSelectedColumnId(2)
      .addSelectedColumnId(3)
      .build
    val infileInfo = FileInfo.newBuilder()
      .addFilePath(
        s"file:///${System.getProperty(
            "user.dir")}/input/datasampleTimeAgg.csv"
        .replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .addField(
        FieldInfo.newBuilder()
          .setKey("PRCS_YEAR")
          .setType(FieldInfo.FieldType.STRING)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("PRCS_MON")
          .setType(FieldInfo.FieldType.STRING)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("PRCS_DAY")
          .setType(FieldInfo.FieldType.STRING)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("PRCS_HH")
          .setType(FieldInfo.FieldType.STRING)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("PRCS_MIN")
          .setType(FieldInfo.FieldType.STRING)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("LINK_ID")
          .setType(FieldInfo.FieldType.STRING)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("PRCS_SPD")
          .setType(FieldInfo.FieldType.DOUBLE)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("PRCS_TRV_TIME")
          .setType(FieldInfo.FieldType.DOUBLE)
          .build())
      .build
    val outfileInfo = FileInfo.newBuilder()
      .addFilePath(
          s"file:///${System.getProperty("user.dir")}/output/result_traffic.csv"
          .replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.reduction.ColumnRemoveOperator")
      .setRemoveCol(removeSelectedColInfo)
      .build
    val operator2 = StreamOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.operator.integration.ConcatAndReshapeOperator")
      .setReshapeWithConcat(concatInfo)
      .build
    val operator3 = StreamOperatorInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.operator.reduction.FilterUsingSqlOperator")
      .setFilterUsingSql(filterUsingSqlInfo)
      .build
    val operator4 = StreamOperatorInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingInfo)
      .build
    val operator5 = StreamOperatorInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.component.operator.transformation.PivotOperator")
      .setPivot(pivotInfo)
      .build
    val operator6 = StreamOperatorInfo.newBuilder()
      .setId(7)
      .setPrevId(6)
      .setClsName("ksb.csle.component.operator.cleaning.MissingValueImputeOperator")
      .setMissingValueImputation(mviInfo)
      .build
    val operator7 = StreamOperatorInfo.newBuilder()
      .setId(8)
      .setPrevId(7)
      .setClsName("ksb.csle.component.operator.integration.TimeIndexAddOperator")
      .setAddTimeIndexColumn(addTimeIndexInfo)
      .build
    val operator8 = StreamOperatorInfo.newBuilder()
      .setId(9)
      .setPrevId(8)
      .setClsName("ksb.csle.component.operator.reduction.TimeWindowAggregateOperator")
      .setAggregateTimeWindow(aggregateTimeWindowInfo)
      .build
    val operator9 = StreamOperatorInfo.newBuilder()
      .setId(10)
      .setPrevId(9)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(selectedColumnsInfo)
      .build
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)
    val writer = BatchWriterInfo.newBuilder()
      .setId(11)
      .setPrevId(10)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
          SparkRunnerInfo.getDefaultInstance)
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(SimpleBatchOrStreamControllerInfo.getDefaultInstance)
    val dataEngineInfo = BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .addOperator(operator7)
      .addOperator(operator8)
      .addOperator(operator9)
      .setRunner(runner)
      .build

    val runType = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()

    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("DataEngine")
            .setBatchToBatchStreamEngine(dataEngineInfo))
      .build
  }
}
