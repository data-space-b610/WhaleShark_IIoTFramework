package ksb.csle

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FileInfo._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._

import ksb.csle.examples.ingestion._

import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.proto.WorkflowProto.WorkflowInfo
import ksb.csle.tools.client.SimpleCsleClient
import ksb.csle.common.proto.WorkflowProto._

package object examples {

  object SimpleClient extends Logging {

    def submit[P](workflow: P): Unit = submit("localhost", 19999, workflow)

    def submit[P](host: String, port: Int, workflow: P): Unit = {
      val client: SimpleCsleClient = new SimpleCsleClient(host, port)
      try {
        val workflowJson = workflow match {
          case x: WorkflowInfo =>
            ProtoUtils.msgToJson(x)
          case x: String => x
        }
        logger.info("workflow: " + workflowJson)

        Try (client.submit(workflowJson, "leeyh@etri.re.kr")) match {
          case Success(rsp) => logger.info("submit success: " + rsp)
          case Failure(e) => logger.error("submit error", e)
        }
      } catch {
        case e: Exception => logger.error(e.getMessage)
      } finally {
        client.close()
      }
    }

    def stop(host: String, port: Int, workflowId: String, engineId: Int): Unit = {
      val client: SimpleCsleClient = new SimpleCsleClient(host, port)
      try {
        Try (client.stop(workflowId, engineId)) match {
          case Success(rsp) => logger.info("stop success: " + rsp)
          case Failure(e) => logger.error("stop error", e)
        }
      } catch {
        case e: Exception => logger.error(e.getMessage)
      } finally {
        client.close()
      }
    }
  }
}
