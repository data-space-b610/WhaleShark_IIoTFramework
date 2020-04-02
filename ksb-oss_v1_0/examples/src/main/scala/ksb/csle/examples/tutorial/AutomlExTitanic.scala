package ksb.csle.examples.tutorial

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.BatchControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.OndemandOperatorProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.AutoSparkMlProto._
import ksb.csle.common.proto.DatasourceProto.FileInfo._
import ksb.csle.common.proto.StreamOperatorProto.FilterInfo.Condition

import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.common.utils.resolver.PathResolver

import ksb.csle.tools.client._

object AutomlExTitanic extends Logging {
  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])
    val client = new SimpleCsleClient("localhost", 19999)
    Try (client.submit(
        workflowJson,
        "ksbuser@etri.re.kr",
        this.getClass.getSimpleName.replace("$",""),
        this.getClass.getSimpleName.replace("$",""))) match {
      case Success(id) => logger.info("submit success:" + id)
      case Failure(e) => logger.error("submit error", e)
    }
    client.close()
  }

  private def workflow: WorkflowInfo = {
    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addEngines(EngineInfo.newBuilder()
          .setPrevId(0)
          .setId(1)
          .setEngineNickName("preprocessing")
          .setBatchToBatchStreamEngine(ppEnginParam))
      .addEngines(EngineInfo.newBuilder()
          .setPrevId(1)
          .setId(2)
          .setEngineNickName("automl")
          .setExternalEngine(automlEngineParam))
      .addRuntypes(RunType.newBuilder()
          .setId(1)
          .setPeriodic(Periodic.ONCE))
      .addRuntypes(RunType.newBuilder()
          .setId(2)
          .setPeriodic(Periodic.ONCE))
      .build()
  }

  private def ppEnginParam = {
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(
        SimpleBatchOrStreamControllerInfo.newBuilder()
          .setOperationPeriod(1))

    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(FileInfo.newBuilder()
          .addFilePath("dataset/input/titanic_train.csv")
          .setFileType(FileType.CSV)
          .setHeader(true))

    val writer = BatchWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(FileInfo.newBuilder()
          .addFilePath("/tmp/titanic_train.parquet")
          .setHeader(true)
          .setFileType(FileType.PARQUET)
          .setSaveMode(SaveMode.OVERWRITE))
//    val writer = BatchWriterInfo.newBuilder()
//      .setId(1)
//      .setPrevId(0)
//      .setClsName("ksb.csle.component.writer.StdoutWriter")
//      .setStdoutWriter(StdoutWriterInfo.getDefaultInstance())

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(SparkRunnerInfo.newBuilder()
          .setSparkArgs(SparkArgs.newBuilder()
              .setDriverMemory("1g")
              .setExecuterMemory("1g")
              .setExecutorCores("4")))

    // drop 'PassengerId', 'Name' and 'Ticket' columns.
    val operator1 = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(SelectColumnsInfo.newBuilder()
          .addAllSelectedColumnName(
              Seq("Survived", "Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked").asJava))
      .build()

    // filter out if 'Age' <= 0. (177 rows deleted)
    val operator2 = StreamOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.operator.reduction.FilterOperator")
      .setFilter(FilterInfo.newBuilder()
          .setColName("Age")
          .setCondition(Condition.LARGE_THAN)
          .setValue(0))
      .build()

    // filter out if 'Embarked' is missed. (2 rows deleted)
    val operator3 = StreamOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.operator.reduction.FilterOperator")
      .setFilter(FilterInfo.newBuilder()
          .setColName("Embarked")
          .setCondition(Condition.LIKE)
          .setPattern("C|Q|S"))
      .build()

    // change data type of 'Age'; double -> integer.
    val operator4 = StreamOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.operator.transformation.ChangeColumnDataTypeOperator")
      .setChangeColumnDataType(ChangeColumnDataTypeInfo.newBuilder()
          .setColumName("Age")
          .setDataType(FieldType.INTEGER))
      .build()

    // index string column; 'Sex', 'Embarked'.
    val operator5 = StreamOperatorInfo.newBuilder()
      .setPrevId(4)
      .setId(5)
      .setClsName("ksb.csle.component.operator.transformation.StringIndexOperator")
      .setStringIndex(StringIndexInfo.newBuilder()
          .setSrcColumnName("Sex")
          .setDestColumnName("ISex")
          .addMore(StringIndexParam.newBuilder()
              .setSrcColumnName("Embarked")
              .setDestColumnName("IEmbarked")))
      .build()

    // add 'features' vector column for AutoML.
    val operator6 = StreamOperatorInfo.newBuilder()
      .setPrevId(5)
      .setId(6)
      .setClsName("ksb.csle.component.operator.transformation.VectorizeColumnOperator")
      .setVectorizeColumn(VectorizeColumnInfo.newBuilder()
          .setSrcColumnNames("Pclass, Age, SibSp, Parch, Fare, ISex, IEmbarked")
          .setDestColumnName("features"))
      .build()

    // rename column name for AutoML; 'Survived' -> 'label'.
    val operator7 = StreamOperatorInfo.newBuilder()
      .setPrevId(6)
      .setId(7)
      .setClsName("ksb.csle.component.operator.transformation.RenameColumnOperator")
      .setRenameColumn(RenameColumnInfo.newBuilder()
          .setExistingName("Survived")
          .setNewName("label"))
      .build()

    BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setRunner(runner)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .addOperator(operator7)
      .setWriter(writer)
      .build()
  }

  private def automlEngineParam = {
    val ksbHome = System.getenv("KSB_HOME")

    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(FileInfo.newBuilder()
          .addFilePath("/tmp/titanic_train.parquet")
          .setHeader(true))

    val writer = BatchWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(FileInfo.newBuilder()
          .addFilePath("/tmp/titanic_automl")
          .setHeader(true))

    val runner = BatchRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.PySparkRunner")
      .setPySparkMLRunner(PySparkMLRunnerInfo.newBuilder()
          .setPyEntryPath(s"file:////$ksbHome/pyML/autosparkml/bridge/call_trainer.py")
          .setInJson(false)
          .setSparkArgs(SparkArgs.newBuilder()
              .setExecuterMemory("2g")
              .setDriverMemory("1g")
              .setNumExecutors("4")))

    val autoMLInfo = AutoSparkMLInfo.newBuilder()
      .setType(AutoSparkMLInfo.Type.Classification)
      .build()
    val operator = BatchOperatorInfo.newBuilder
      .setId(2)
      .setPrevId(1)
      .setClsName("DummyClass")
      .setAutoMLTrainer(autoMLInfo)

    val controller = BatchControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.PySparkMLTrainer")
      .setPyMLTrainer(SimpleBatchControllerInfo.newBuilder())

    ExternalEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setRunner(runner)
      .addOperator(operator)
      .build
  }
}
