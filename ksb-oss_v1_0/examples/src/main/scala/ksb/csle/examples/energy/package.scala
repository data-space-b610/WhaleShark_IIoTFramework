package ksb.csle.examples

import org.apache.logging.log4j.scala.Logging
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.tools.client.SimpleCsleClient
import ksb.csle.common.proto.WorkflowProto.WorkflowInfo

package object energy {

  object SimpleClient extends Logging {
//    val client: SimpleCsleClient = new SimpleCsleClient("129.254.169.239", 19999)
//    val client: SimpleCsleClient = new SimpleCsleClient("129.254.169.215", 19999)
    val client: SimpleCsleClient = new SimpleCsleClient("csle1", 19999)

    def submit[P](workflow: P): Unit = {
      try {
        val workflowJson = workflow match {
          case x: WorkflowInfo =>
            ProtoUtils.msgToJson(x)
          case x: String => x
        }
        logger.info("workflow: " + workflowJson)
        client.submit(workflowJson)
      } catch {
        case e: Exception => logger.error(e.getMessage)
      } finally {
        client.close()
      }
    }
  }
    
}