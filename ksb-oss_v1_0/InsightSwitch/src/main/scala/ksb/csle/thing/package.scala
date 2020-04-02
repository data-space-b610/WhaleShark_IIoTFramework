package ksb.csle

import org.slf4j.Logger
import org.slf4j.LoggerFactory

package object thing {
  
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
  
  val QUERY_URI = "http://129.254.169.220:18080/query"
  val DEFAULT_POOL_SIZE: Int = 100
  val DEFAULT_KEEPALIVE_TIME: Long = 60
  val HOME_PATH = "./dist"
  val CONF_FILE_PATH = "/insightSwitch.xml"
  val LOG_CONF_FILE_PATH = "/log4j.xml"
  val DEFAULT_SENSING_PERIOD = 10000
  val CONTENT_TYPE: String = "application/json"
  val RETRY_INTERVAL: Long = 5000

}