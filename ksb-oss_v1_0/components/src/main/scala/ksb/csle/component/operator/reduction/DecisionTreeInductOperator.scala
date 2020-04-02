package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tree.DecisionTreeModelReadWrite
import org.apache.spark.mllib.tree.model.Node

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.DecisionTreeInductionInfo
import ksb.csle.common.proto.StreamOperatorProto.DecisionTreeInductionInfo.ImpurityType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs Decision Tree Induction. It selects the most relevant
 * columns.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.DecisionTreeInductionInfo]]
 *          DecisionTreeInductionInfo contains attributes as follows:
 *          - labelName: Column name of label (required)
 *          - maxDepth: Size of the decision Tree (required)
 *          - minInfoGain: Minimum information gain for a split to be considered
 *                         at a tree node (required)
 *          - minInstancesPerNode: Minimum number of instances each child must
 *                                 have after split (required)
 *          - maxBins: Maximum number of bins used for discretizing continuous
 *                     features and for choosing how to split on features at 
 *                     each node (required)
 *          - cacheNodeIds: Parameter specifies whether caching is applied or
 *                          not (required)
 *                          If false, the algorithm will pass trees to executors
 *                          to match instances with nodes.
 *                          If true, the algorithm will cache node IDs for each
 *                          instance.
 *          - checkpointInterval: Checkpoint interval(>= 1) or disable checkpoint(-1)
 *                                (required)
 *          - impurityType: Inpurity type used for information gain calculation.
 *                          Enum(GINI, ENTROPY) (required)
 *
 *  ==DecisionTreeInductionInfo==
 *  {{{
 *  message DecisionTreeInductionInfo {
 *  required string labelName = 3;
 *  required int32 maxDepth = 4;
 *  required double minInfoGain = 5;
 *  required int32 minInstancesPerNode = 6 [default = 1];
 *  required int32 maxBins = 7;
 *  required bool cacheNodeIds = 8 [default = false];
 *  required int32 checkpointInterval = 9 [default = 10];
 *  required ImpurityType impurityType = 10 [default = GINI];
 *  enum ImpurityType {
 *      GINI = 0;
 *      ENTROPY = 1;
 *  }
 *  }
 *  }}}
 */
class DecisionTreeInductOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.DecisionTreeInductionInfo =
    o.getDecisionTreeInduction

  /**
   * validates DecisionTreeInduction info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame) {
    try {
      if(!df.columns.contains(p.getLabelName))
        throw new DataException("Label column is not exist")
    } catch {
      case e: DataException => throw e
      case e: Exception     => throw new DataException(s"validate Error : ", e)
    }
  }

  /**
   * Run decisionTreeInduction operation using following params.
   */
  @throws(classOf[KsbException])
  private def decisionTreeInduction(df: DataFrame): DataFrame = {
    validate(df)
    val labelName: String = p.getLabelName
    try {
      val stringColumns: Array[String] =  getStringColumns(df)
      val index_transformers = stringColumns.map(
        cname => new StringIndexer()
          .setInputCol(cname)
          .setOutputCol(s"${cname}_index")
      )
      val stringColumns_index = stringColumns.map(cname => s"${cname}_index")
      val numbericColumns: Array[String] = getNumbericColumns(df)
      val columns: Array[String] = stringColumns_index ++ numbericColumns
      val assembler = new VectorAssembler()
        .setInputCols(columns)
        .setOutputCol("features")
      val labelIndexer = new StringIndexer()
        .setInputCol(labelName)
        .setOutputCol("indexedLabel")
        .fit(df)
      val dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features")
        .setMaxDepth(p.getMaxDepth)
        .setMinInfoGain(p.getMinInfoGain)
        .setMinInstancesPerNode(p.getMinInstancesPerNode)
        .setMaxBins(p.getMaxBins)
        .setCacheNodeIds(p.getCacheNodeIds)
        .setCheckpointInterval(p.getCheckpointInterval)
        .setImpurity(p.getImpurityType.getValueDescriptor.getName.toLowerCase())
      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)
      val stages =
        index_transformers :+ assembler :+ labelIndexer :+ dt :+ labelConverter
      val pipeline = new Pipeline()
        .setStages(stages)
      val model = pipeline.fit(df)
      val predictions = model.transform(df)
      val treeModel = model.stages(stages.size - 2)
        .asInstanceOf[DecisionTreeClassificationModel]
      logger.info("Learned classification tree model:\n"
          + treeModel.toDebugString)
      val result = df.select(
          getSelectAttributeList(
              treeModel.featureImportances.toArray, columns)
              .toList.map(col):_*)
      result.show
      result
    } catch {
      case e: Exception => throw new ProcessException(
          s"DecisionTreeInduction Process Error : ", e)
    }
  }

  private def getSelectAttributeList(featureImportances: Array[Double],
      columns: Array[String]): Array[String] = {
    println("featureImportances")
    var selectList = new Array[String](0)
    for(i <- 0 until featureImportances.length) {
      if(featureImportances(i) != 0) {
        val columnName = columns(i).replace("_index", "")
        selectList :+= columnName
        println(columnName + " : " + featureImportances(i))
      }
    }
    selectList
  }

  private def getStringColumns(df: DataFrame): Array[String] = {
    var result: Array[String] = new Array[String](0)
    var index: Int = 0
    for (field <- df.schema.fields) {
      if(field.dataType.isInstanceOf[StringType]) {
        if(!field.name.equals(p.getLabelName)) {
          result :+= field.name
        }
      }
    }
    result
  }

  private def getNumbericColumns(df: DataFrame): Array[String] = {
    var result: Array[String] = new Array[String](0)
    var index: Int = 0
    for (field <- df.schema.fields) {
      if(!field.dataType.isInstanceOf[StringType]) {
        if(!field.name.equals(p.getLabelName)) {
          result :+= field.name
        }
      }
    }
    result
  }

  /**
   * Operates Aggregate.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = decisionTreeInduction(df)
}

object DecisionTreeInductOperator {
  def apply(o: StreamOperatorInfo): DecisionTreeInductOperator =
    new DecisionTreeInductOperator(o)
}