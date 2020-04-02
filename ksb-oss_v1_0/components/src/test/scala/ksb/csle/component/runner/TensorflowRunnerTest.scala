package ksb.csle.component.runner

import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.BatchOperatorProto.DLTrainOperatorInfo

final class TensorflowRunnerTest extends UnitSpec {
}

object TensorflowRunnerTest {

  val getParams: DLTrainOperatorInfo = DLTrainOperatorInfo.newBuilder()
//        .setIsTrain(true) // not a member, see workflow_proto.proto in common project.
//        .setInputPath(s"file:///${System.getProperty("user.dir")}/datasets/rnn/trainset.csv".replaceAll("\\\\", "/"))
        .setModelPath(s"file:///${System.getProperty("user.dir")}/models/rnn/model.ckpt".replaceAll("\\\\", "/"))
//        .setOutputPath(s"file:///${System.getProperty("user.dir")}/output/accracy/rnn/rnn_accuracy.csv".replaceAll("\\\\", "/"))
        .addAdditionalParams(
            ParamPair.newBuilder()
            .setParamName("name")
            .setParamValue("value"))
        .build()
}
