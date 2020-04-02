package ksb.csle.examples

import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.proto.WorkflowProto.WorkflowInfo
import ksb.csle.tools.client.SimpleCsleClient
import ksb.csle.common.proto.WorkflowProto._

package object didentification {

  object SimpleClient extends Logging {
    val client: SimpleCsleClient = new SimpleCsleClient("localhost", 19999)

    def submit[P](workflow: P): Unit = {
      try {
        val workflowJson = workflow match {
          case x: WorkflowInfo =>
            ProtoUtils.msgToJson(x)
          case x: String => x
        }
        logger.info("workflow: " + workflowJson)
  
        val client: SimpleCsleClient = new SimpleCsleClient("localhost", 19999)
        client.submit(workflowJson)
      } catch {
        case e: Exception => logger.error(e.getMessage)
      } finally {
        client.close()
      }
    }
  }
  
}